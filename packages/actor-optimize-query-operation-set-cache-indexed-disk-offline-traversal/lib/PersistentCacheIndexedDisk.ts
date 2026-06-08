import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';
import { ActionContext } from '@comunica/core';
import type { ISourceState, ICacheMetrics, IPersistentCache } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import type * as RDF from '@rdfjs/types';
import type { Quad } from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator } from 'asynciterator';
import { ClassicLevel } from 'classic-level';
import { LRUCache } from 'lru-cache';
import type { Pattern } from 'quadstore';
import { Quadstore } from 'quadstore';
import { DataFactory } from 'rdf-data-factory';

// Maintain a single, application-wide database reference outside the class instance. This is
// to prevent file-locks from breaking the application when a new instance of a queryEngine is made.
let globalDbInstance: ClassicLevel | null = null;
let globalStoreInstance: Quadstore | null = null;
let activeConnectionCount = 0;

function getSharedQuadstore(serializationLoc: string, dataFactory: any): { store: Quadstore; readyPromise: Promise<void> } {
  activeConnectionCount++;

  if (!globalStoreInstance) {
    globalDbInstance = new ClassicLevel(serializationLoc);
    globalStoreInstance = new Quadstore({
      backend: <any> globalDbInstance,
      dataFactory,
      indexes: [
        [ 'graph', 'subject', 'predicate', 'object' ],
        [ 'graph', 'predicate', 'object', 'subject' ],
        [ 'graph', 'object', 'subject', 'predicate' ],
      ],
    });

    return {
      store: globalStoreInstance,
      readyPromise: globalStoreInstance.open().catch((err) => {
        console.error('FATAL: Failed to open persistent quadstore cache:', err);
        process.exit(1);
      }),
    };
  }

  // Connection already open! Instantly return it.
  return {
    store: globalStoreInstance,
    readyPromise: Promise.resolve(),
  };
}

export class PersistentCacheIndexedDisk implements IPersistentCache<ISourceState, ISourceState> {
  private readonly maxNumTriplesDisk: number;
  private readonly maxNumTriplesInMemory: number;
  private readonly activeIngestions = new Map<string, Promise<void>>();

  private readonly hotLRUCacheDocuments: LRUCache<string, ISourceState>;
  // A tracker to do pre-filtering of URLs before putting into the hot-cache.
  private readonly hotCachePolicy: 'lru' | 'lru-filtered';
  private readonly previouslyDereferenced?: LRUCache<string, number>;
  private readonly decayThreshold = 10000;
  private nAccesses = 0;

  private readonly lruCacheDiskBacked: LRUCache<string, string>;
  private readonly metadataKeysToCache: string[];
  private readonly savedMetadata = new Map<string, Record<string, any>>();
  private readonly sizeMap = new Map<string, number>();

  private readonly dataFactory = new DataFactory();
  private readonly bindingsFactory = new BindingsFactory(this.dataFactory);
  private readonly algebraFactory = new AlgebraFactory(this.dataFactory);

  private isTracking = false;
  private cacheMetrics: ICacheMetrics;

  private readonly serializationLoc: string;
  private store: Quadstore;
  private readyPromise: Promise<void>;

  public constructor(args: IPersistentCacheSourceStateNumTriplesArgs) {
    this.maxNumTriplesDisk = args.maxNumTriples;
    this.maxNumTriplesInMemory = args.maxTriplesInMemory;
    this.serializationLoc = args.serializationLoc ?? `${__dirname}/../cache/`;

    const connection = getSharedQuadstore(this.serializationLoc, this.dataFactory);
    this.store = connection.store;
    this.readyPromise = connection.readyPromise;

    // LRU cache storing traversal entries of disk-backed data.
    // Also used to determine eviction of quads in the disk-based store
    this.lruCacheDiskBacked = new LRUCache<string, string>({
      maxSize: this.maxNumTriplesDisk,
      sizeCalculation: (value, key) => this.sizeMap.get(key) || 1,
      dispose: this.onDispose.bind(this),
    });

    this.hotLRUCacheDocuments = new LRUCache<string, ISourceState>({
      maxSize: this.maxNumTriplesInMemory,
      sizeCalculation: (value, key) => this.sizeMap.get(key) || 1,
      dispose: this.onDisposeHotCache.bind(this),
    });

    this.hotCachePolicy = args.hotCachePolicy;
    if (this.hotCachePolicy === 'lru-filtered') {
      this.previouslyDereferenced = new LRUCache<string, number>({ max: 50000 });
    }

    this.metadataKeysToCache = args.metadataKeysToCache ?? [ 'traverse', 'offlineTraversal' ];
    this.cacheMetrics = this.resetMetrics();

    // Force close on process termination
    const cleanup = () => {
      this.store.close().catch(() => {});
    };
    process.on('exit', cleanup);
    process.on('SIGINT', () => {
      cleanup(); process.exit(2);
    });
    process.on('SIGTERM', () => {
      cleanup(); process.exit(15);
    });
  }

  /**
   * Generates a safe URI to use in the Graph position for this document
   */
  private getCacheGraphNode(url: string): RDF.NamedNode {
    return this.dataFactory.namedNode(`urn:cache:doc:${encodeURIComponent(url)}`);
  }

  private canAddToHotCache(url: string) {
    if (this.hotCachePolicy === 'lru' || !this.previouslyDereferenced) {
      return true;
    }
    const visits = (this.previouslyDereferenced.get(url) || 0) + 1;
    this.previouslyDereferenced.set(url, visits);
    this.nAccesses++;
    // Decay frequency using sliding window
    if (this.nAccesses >= this.decayThreshold) {
      for (const [ key, count ] of this.previouslyDereferenced.entries()) {
        const halved = Math.floor(count / 2);
        if (halved === 0) {
          this.previouslyDereferenced.delete(key);
        } else {
          this.previouslyDereferenced.set(key, halved);
        }
      }
      this.nAccesses = 0;
    }

    // Can add to cache if atleast used twice within sliding window
    if (visits >= 2) {
      return true;
    }
    return false;
  }

  public async getMany(keys: string[]): Promise<(ISourceState | undefined)[]> {
    return Promise.all(keys.map(key => this.get(key)));
  }

  public async get(key: string): Promise<ISourceState | undefined> {
    await this.readyPromise;

    const ongoingIngestion = this.activeIngestions.get(key);
    if (ongoingIngestion) {
      await ongoingIngestion;
    }

    const cachedState = this.hotLRUCacheDocuments.get(key);
    if (cachedState) {
      if (this.isTracking) {
        this.cacheMetrics.additionalMetrics!.hotCache.hits++;
      }
      return cachedState;
    }

    const cacheGraph = this.getCacheGraphNode(key);
    const out = await this.store.get({ graph: cacheGraph }, { limit: 1 });
    if (out.items.length > 0) {
      if (this.isTracking) {
        this.cacheMetrics.hits++;
      }
      const rehydratedState = this.createSourceStateFromDisk(key, cacheGraph);

      const storedMeta = this.savedMetadata.get(key);
      if (storedMeta === undefined) {
        throw new Error(`Could not find saved metadata for cache entry ${key}`);
      }

      rehydratedState.metadata = { ...rehydratedState.metadata, ...storedMeta };
      // If (traverse === undefined){
      //   throw new Error("Could not find traverse metadata for cache entry that exists within "+
      //     "the disk-based store"
      //   );
      // }
      // rehydratedState.metadata.traverse = traverse;
      if (this.canAddToHotCache(key)) {
        this.hotLRUCacheDocuments.set(key, rehydratedState);
      }
      return rehydratedState;
    }

    if (this.isTracking) {
      this.cacheMetrics.misses++;
    }
    return undefined;
  }

  public async set(key: string, value: ISourceState): Promise<void> {
    await this.readyPromise;

    if (this.activeIngestions.has(key)) {
      return this.activeIngestions.get(key);
    }

    const ingestionPromise = this._set(key, value).finally(() => {
      this.activeIngestions.delete(key);
    });

    this.activeIngestions.set(key, ingestionPromise);
    await ingestionPromise;
  }

  private async _set(key: string, value: ISourceState): Promise<void> {
    const cacheGraph = this.getCacheGraphNode(key);

    const quadStream = value.source.queryQuads(
      this.algebraFactory.createPattern(
        this.dataFactory.variable('s'),
        this.dataFactory.variable('p'),
        this.dataFactory.variable('o'),
        this.dataFactory.variable('g'),
      ),
      new ActionContext(),
    );

    let nTriples = 0;
    const transformStream = quadStream.map((quad) => {
      nTriples++;
      return this.dataFactory.quad(quad.subject, quad.predicate, quad.object, cacheGraph);
    });

    await this.store.putStream(transformStream);

    // After ingestion to disk is done we update the in-memory caches
    this.sizeMap.set(key, nTriples);

    const extractedMetadata: Record<string, any> = {};
    for (const metaKey of this.metadataKeysToCache) {
      if (value.metadata[metaKey] !== undefined) {
        extractedMetadata[metaKey] = value.metadata[metaKey];
      }
    }
    this.savedMetadata.set(key, extractedMetadata);

    // Register cache entry to manage LRU-eviction of quads in disk-based cache
    this.lruCacheDiskBacked.set(key, '');

    // Add entry to hot cache
    const rehydratedState = this.createSourceStateFromDisk(key, cacheGraph);
    if (this.canAddToHotCache(key)) {
      this.hotLRUCacheDocuments.set(key, rehydratedState);
    }
  }

  private createSourceStateFromDisk(key: string, cacheGraph: RDF.NamedNode): ISourceState {
    const sanitizeTerm = <T extends RDF.Term>(term?: T): T | undefined =>
      term?.termType === 'Variable' ? undefined : term;

    const quadSource: any = {
      match: (
        subject?: RDF.Quad_Subject | undefined,
        predicate?: RDF.Quad_Predicate | undefined,
        object?: RDF.Quad_Object | undefined,
        graph?: RDF.Quad_Graph | undefined,
      ): AsyncIterator<Quad> => <AsyncIterator<Quad>><unknown> this.store.match(
        sanitizeTerm(subject),
        sanitizeTerm(predicate),
        sanitizeTerm(object),
        cacheGraph,
      ).map((quad) => {
        quad.graph = this.dataFactory.defaultGraph();
        return quad;
      }),
      countQuads: async(
        subject?: RDF.Quad_Subject | undefined,
        predicate?: RDF.Quad_Predicate | undefined,
        object?: RDF.Quad_Object | undefined,
        graph?: RDF.Quad_Graph | undefined,
      ): Promise<number> =>
        // We use Quadstore's native countQuads, replacing the requested graph with our cached graph boundary
        this.store.countQuads(
          sanitizeTerm(subject),
          sanitizeTerm(predicate),
          sanitizeTerm(object),
          cacheGraph,
        ),
    };
    // Add metadata back
    const cachedMetadata = this.savedMetadata.get(key);

    return {
      link: { url: key },
      metadata: {
        traverse: [],
        state: new MetadataValidationState(),
        cardinality: { value: 0, type: 'estimate' },
        variables: [],
        ...cachedMetadata

      },
      handledDatasets: {},
      cachePolicy: <any> { satisfiesWithoutRevalidation: async() => true },
      source: new QuerySourceRdfJs(
        quadSource,
        this.dataFactory,
        this.bindingsFactory,
      ),
    };
  }

  protected onDispose(value: string, key: string, reason: LRUCache.DisposeReason) {
    if (reason === 'evict' && this.isTracking) {
      this.cacheMetrics.evictions++;
      this.cacheMetrics.evictionsCalculatedSize += this.sizeMap.get(key) ?? 1;
      this.cacheMetrics.evictionPercentage =
        (this.cacheMetrics.evictionsCalculatedSize / this.maxNumTriplesDisk) * 100;
      this._delete(key);
    }
  }

  protected onDisposeHotCache(value: ISourceState, key: string, reason: LRUCache.DisposeReason) {
    if (reason === 'evict' && this.isTracking) {
      const cacheMetricsHot = this.cacheMetrics.additionalMetrics!.hotCache;
      cacheMetricsHot.evictions++;
      cacheMetricsHot.evictionsCalculatedSize += this.sizeMap.get(key) ?? 1;
      cacheMetricsHot.evictionPercentage =
        (cacheMetricsHot.evictionsCalculatedSize / this.maxNumTriplesInMemory) * 100;
    }
  }

  public async has(key: string): Promise<boolean> {
    await this.readyPromise;
    if (this.hotLRUCacheDocuments.has(key)) {
      return true;
    }
    const out = await this.store.get(
      <Pattern> <any> this.algebraFactory.createPattern(
        this.dataFactory.variable('s'),
        this.dataFactory.variable('p'),
        this.dataFactory.variable('o'),
        this.getCacheGraphNode(key),
      ),
      { limit: 1 },
    );
    return out.items.length > 0;
  }

  public async delete(key: string): Promise<boolean> {
    await this.readyPromise;

    // If the key we want to delete is still ingesting we need to wait for that
    // to prevent dangling entries
    const ongoingIngestion = this.activeIngestions.get(key);
    if (ongoingIngestion) {
      try {
        await ongoingIngestion;
      } catch {
        // Ignore the ingestion error here. We must proceed with the
        // deletion cascade to clean up any partially written disk data.
      }
    }
    this.sizeMap.delete(key);
    return this._delete(key);
  }

  private _delete(key: string): Promise<boolean> {
    this.lruCacheDiskBacked.delete(key);
    this.savedMetadata.delete(key);
    this.hotLRUCacheDocuments.delete(key);

    const removalStream = this.store.deleteGraph(
      this.getCacheGraphNode(key),
    );
    return new Promise<boolean>((resolve, reject) => {
      removalStream.on('end', () => resolve(true));
      removalStream.on('error', err => reject(err));
    });
  }

  /**
   * Serializes the in-memory maps and LRU state to disk so the cache can survive restarts
   */
  public async serialize(): Promise<void> {
    await this.readyPromise;
    try {
      const metadataFile = path.join(this.serializationLoc, 'metadata.json');

      const serializedData = {
        // .dump() exports an array of objects representing the exact internal state of the LRU
        lruCacheDiskBacked: this.lruCacheDiskBacked.dump(),
        previouslyDereferenced: this.previouslyDereferenced?.dump(),
        // Convert Maps to array of tuples for JSON serialization
        sizeMap: [ ...this.sizeMap.entries() ],
        savedMetadata: [ ...this.savedMetadata.entries() ],
      };

      await fs.writeFile(metadataFile, JSON.stringify(serializedData), 'utf-8');
      console.log(`Successfully serialized cache metadata to ${metadataFile}`);
    } catch (error) {
      console.error('Failed to serialize cache metadata:', error);
    }
  }

  /**
   * Deserializes the in-memory maps and LRU state from disk
   */
  public async deserialize(): Promise<void> {
    await this.readyPromise;
    try {
      const metadataFile = path.join(this.serializationLoc, 'metadata.json');

      // Check if file exists. If not, this is a fresh start; do nothing.
      try {
        await fs.access(metadataFile);
      } catch {
        return;
      }

      const rawData = await fs.readFile(metadataFile, 'utf-8');
      const parsedData = JSON.parse(rawData);

      // Load the maps first to calculate the sizes of the items being loaded.
      if (parsedData.sizeMap) {
        this.sizeMap.clear();
        for (const [ k, v ] of parsedData.sizeMap) {
          this.sizeMap.set(k, v);
        }
      }

      if (parsedData.savedMetadata) {
        this.savedMetadata.clear(); ;
        for (const [ k, v ] of parsedData.savedMetadata) {
          this.savedMetadata.set(k, v);
        }
      }

      if (parsedData.lruCacheDiskBacked) {
        this.lruCacheDiskBacked.load(parsedData.lruCacheDiskBacked);
      }

      if (parsedData.previouslyDereferenced) {
        this.previouslyDereferenced?.load(parsedData.previouslyDereferenced);
      }

      console.log(`Successfully deserialized cache metadata from ${metadataFile}`);
    } catch (error) {
      console.error('Failed to deserialize cache metadata:', error);
    }
  }

  public async clear(): Promise<void> {
    await this.readyPromise;

    // Wait for ingestion to finish to not leave ghost triples
    const pendingIngestions = [ ...this.activeIngestions.values() ];
    if (pendingIngestions.length > 0) {
      await Promise.allSettled(pendingIngestions);
    }

    // Clear in-memory maps
    this.sizeMap.clear();
    this.savedMetadata.clear();
    this.hotLRUCacheDocuments.clear();
    this.lruCacheDiskBacked.clear();
    this.activeIngestions.clear();

    // Close underlying database to release OS lock
    if (globalStoreInstance) {
      await globalStoreInstance.close();
      globalStoreInstance = null;
      globalDbInstance = null;
    }

    // Destroy the database files now that the lock is released.
    await ClassicLevel.destroy(this.serializationLoc);

    // Spawn new database
    const connection = getSharedQuadstore(this.serializationLoc, this.dataFactory);
    this.store = connection.store;
    this.readyPromise = connection.readyPromise;

    await this.readyPromise;
  }

  public entries(): AsyncIterator<[string, ISourceState]> {
    return new ArrayIterator(this.hotLRUCacheDocuments.entries(), { autoStart: false });
  }

  public async size(): Promise<number> {
    await this.readyPromise;
    const stats = await this.store.countQuads();
    return stats;
  }

  public startSession() {
    this.isTracking = true;
    this.cacheMetrics = this.resetMetrics();
    return this.cacheMetrics;
  }

  public endSession() {
    this.isTracking = false;
    return this.cacheMetrics;
  }

  public resetMetrics(): ICacheMetrics {
    return {
      hits: 0,
      misses: 0,
      evictions: 0,
      evictionsCalculatedSize: 0,
      evictionPercentage: 0,
      additionalMetrics: {
        hotCache: {
          hits: 0,
          misses: 0,
          evictions: 0,
          evictionsCalculatedSize: 0,
          evictionPercentage: 0,
        },
      },
    };
  }
}

export interface IPersistentCacheSourceStateNumTriplesArgs {
  maxNumTriples: number;
  maxTriplesInMemory: number;
  serializationLoc?: string;
  hotCachePolicy: 'lru' | 'lru-filtered';
  metadataKeysToCache?: string[];
}
