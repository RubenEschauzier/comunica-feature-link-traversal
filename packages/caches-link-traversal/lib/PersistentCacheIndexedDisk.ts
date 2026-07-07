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
import { Quadstore } from 'quadstore';
import { DataFactory } from 'rdf-data-factory';
import * as fsSync from 'node:fs';
import * as readline from 'node:readline';
import { pipeline } from 'node:stream/promises';

// Maintain a single, application-wide database reference outside the class instance. This is
// to prevent file-locks from breaking the application when a new instance of a queryEngine is made.
let globalDbInstance: ClassicLevel | null = null;
let globalStoreInstance: Quadstore | null = null;
let activeConnectionCount = 0;

function getSharedQuadstore(serializationLoc: string, dataFactory: any): { store: Quadstore; readyPromise: Promise<void> } {
  activeConnectionCount++;

  if (!globalStoreInstance) {
    globalDbInstance = new ClassicLevel(serializationLoc, {
      // Reduces the in-memory write buffer before flushing to disk (default is often 4MB-8MB)
      writeBufferSize: 4 * 1024 * 1024, 
            
      // Limits how many files LevelDB keeps open simultaneously (reduces OS memory overhead)
      maxOpenFiles: 100,
    });    

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
  private readonly activeDeletions = new Map<string, Promise<void>>();

  private readonly hotLRUCacheDocuments: LRUCache<string, ISourceState>;
  // A tracker to do pre-filtering of URLs before putting into the hot-cache.
  private readonly hotCachePolicy: 'lru' | 'lru-filtered';
  private readonly previouslyDereferenced?: LRUCache<string, number>;
  private readonly decayThreshold = 10000;
  private nAccesses = 0;

  private readonly lruCacheDiskBacked: LRUCache<string, boolean>;
  private readonly metadataKeysToCache: string[];
  private readonly savedMetadata = new Map<string, Record<string, any>>();
  private readonly sizeMap = new Map<string, number>();

  private readonly dataFactory = new DataFactory();
  private readonly bindingsFactory = new BindingsFactory(this.dataFactory);
  private readonly algebraFactory = new AlgebraFactory(this.dataFactory);

  private isTracking = false;
  private cacheMetrics: ICacheMetrics;

  private readonly serializationLoc: string;
  private isClosed: boolean;
  private store: Quadstore;
  private readyPromise: Promise<void>;
  
  public constructor(args: IPersistentCacheIndexedDiskArgs) {
    this.maxNumTriplesDisk = args.maxNumTriples;
    this.maxNumTriplesInMemory = args.maxTriplesInMemory;
    this.serializationLoc = args.serializationLoc ?? `${__dirname}/../cache/`;

    const connection = getSharedQuadstore(this.serializationLoc, this.dataFactory);
    this.store = connection.store;
    this.readyPromise = connection.readyPromise;
    this.isClosed = false;

    // LRU cache storing traversal entries of disk-backed data.
    // Also used to determine eviction of quads in the disk-based store
    this.lruCacheDiskBacked = new LRUCache<string, boolean>({
      maxSize: this.maxNumTriplesDisk,
      sizeCalculation: (value, key) => {
        return this.sizeMap.get(key) || 1
      },
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
    this.metadataKeysToCache = args.metadataKeysToCache ?? [ 'traverse', 'defaultTraversal', 'predicateToLinks' ];

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

    const ongoingDeletion = this.activeDeletions.get(key);
    if (ongoingDeletion){
      await ongoingDeletion;
      return undefined;
    }

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
        console.warn(`Desynchronization detected: Missing metadata for ${key}. Evicting corrupted cache entry.`);
        await this.delete(key);
        
        if (this.isTracking) {
           this.cacheMetrics.hits--;
           this.cacheMetrics.misses++;
        }
        return undefined;
      }

      rehydratedState.metadata = { ...rehydratedState.metadata, ...storedMeta };

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
    if (this.isClosed){
      return;
    }
    await this.readyPromise;

    const ongoingDeletion = this.activeDeletions.get(key);
    if (ongoingDeletion){
      await ongoingDeletion;
    }

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
    const predicateToLinks: Record<string, Set<string>> = {};
    const extractLinks = this.metadataKeysToCache.includes('predicateToLinks');

    const transformStream = quadStream.map((quad) => {
      nTriples++;
      if (extractLinks && quad.object.termType === 'NamedNode') {
        let urlSet = predicateToLinks[quad.predicate.value];
        if (!urlSet) {
          urlSet = new Set<string>();
          predicateToLinks[quad.predicate.value] = urlSet;
        }
        urlSet.add(quad.object.value);
      }
      return this.dataFactory.quad(quad.subject, quad.predicate, quad.object, cacheGraph);
    });

    await this.store.putStream(transformStream);

    // After ingestion to disk is done we update the in-memory caches
    this.sizeMap.set(key, nTriples);

    // Set predicate to links metadata entry
    if (extractLinks) {
      const serializableLinks: Record<string, string[]> = {};
      for (const [predicate, urlSet] of Object.entries(predicateToLinks)) {
        serializableLinks[predicate] = Array.from(urlSet);
      }
      value.metadata.predicateToLinks = serializableLinks;
    }

    const extractedMetadata: Record<string, any> = {};
    for (const metaKey of this.metadataKeysToCache) {
      if (value.metadata[metaKey] !== undefined) {
        extractedMetadata[metaKey] = value.metadata[metaKey];
      }
    }
    this.savedMetadata.set(key, extractedMetadata);

    // Register cache entry to manage LRU-eviction of quads in disk-based cache
    this.lruCacheDiskBacked.set(key, true);

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
        // We use Quadstore's native countQuads, 
        // replacing the requested graph with our cached graph boundary
        // Note that this is an approximation
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

  protected onDispose(value: boolean, key: string, reason: LRUCache.DisposeReason) {
    if (reason === 'evict' && this.isTracking) {
      this.cacheMetrics.evictions++;
      this.cacheMetrics.evictionsCalculatedSize += this.sizeMap.get(key) ?? 1;
      this.cacheMetrics.evictionPercentage =
        (this.cacheMetrics.evictionsCalculatedSize / this.maxNumTriplesDisk) * 100;
    }

    this.sizeMap.delete(key);
    this.savedMetadata.delete(key);
    this.hotLRUCacheDocuments.delete(key);

    if (reason === 'evict' || reason === 'set' || reason === 'expire') {
      this.triggerGraphDeletion(key);
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
      {
        graph: this.getCacheGraphNode(key)
      },
      { limit: 1 },
    );
    return out.items.length > 0;
  }

  public async delete(key: string): Promise<boolean> {
    await this.readyPromise;

    const ongoingIngestion = this.activeIngestions.get(key);
    if (ongoingIngestion) {
      try { 
        await ongoingIngestion; 
      } catch {
        
      }
    }

    const existed = this.lruCacheDiskBacked.has(key);
    if (!existed) {
      return false;
    }

    this.lruCacheDiskBacked.delete(key);
    await this.triggerGraphDeletion(key);
    return true;
  }

  /**
   * Stream the cache to jsonl file
   */
  public async serialize(): Promise<void> {
    // Close the cache from ingestions, to ensure we don't miss serializing a metadata entry
    // added after the query finalized
    this.isClosed = true;

    // Wait for all current ingestion runs
    await this.readyPromise;
    if (this.activeIngestions.size > 0) {
      await Promise.allSettled([...this.activeIngestions.values()]);
    }

    const metadataFile = path.join(this.serializationLoc, 'metadata.jsonl');
    let nSerialized = 0
    try {
      const writeStream = fsSync.createWriteStream(metadataFile, { encoding: 'utf-8' });
      writeStream.on('error', (err) => console.error('writeStream error:', err));

      const generateMetadata = async function* (this: PersistentCacheIndexedDisk) {
        try {
          console.log("Yielding lruCacheBacked dump");
          const lruLine = JSON.stringify({ type: 'lruDisk', data: this.lruCacheDiskBacked.dump() }) + '\n';
          yield lruLine;

          if (this.previouslyDereferenced) {
            console.log("Yielding previously dereferenced cache metadata for serialization");
            yield JSON.stringify({ type: 'lruFiltered', data: this.previouslyDereferenced.dump() }) + '\n';
          }
          console.log("Finished yielding previously dereferenced cache metadata for serialization");

          for (const [key, value] of this.sizeMap.entries()) {
            nSerialized++;
            yield JSON.stringify({ type: 'sizeMap', key, value }) + '\n';
          }
          console.log("Finished yielding sizeMap cache metadata for serialization");

          for (const [key, value] of this.savedMetadata.entries()) {
            yield JSON.stringify({ type: 'savedMetadata', key, value }) + '\n';
          }

          console.log("Finished yielding savedMetadata cache metadata for serialization");
        } catch (err) {
          console.log(err);
          throw err;
        }
      };

      const gen = generateMetadata.bind(this)();

      try {
        await pipeline(gen, writeStream);
      } catch (err) {
        console.error('pipeline rejected with:', err);
        throw err;
      }

      console.log(`Successfully serialized ${nSerialized} cache metadata to ${metadataFile}`);
    } catch (error) {
      console.error('Failed to serialize cache metadata:', error);
      await fs.unlink(metadataFile).catch(() => {
        console.warn('Failed to clean up partial metadata file');
      });
    }
  }
  
  public async deserialize(): Promise<void> {
    await this.readyPromise;
    const metadataFile = path.join(this.serializationLoc, 'metadata.jsonl');

    try {
      await fs.access(metadataFile);
    } catch {
      return;
    }

    this.sizeMap.clear();
    this.savedMetadata.clear();

    try {
      // Deserialize metadata
      const readStreamMeta = fsSync.createReadStream(metadataFile, { encoding: 'utf-8' });
      const rlMeta = readline.createInterface({ input: readStreamMeta, crlfDelay: Infinity });

      let nDeserialized = 0;
      const startTime = performance.now();

      for await (const line of rlMeta) {
        if (!line.trim()) continue;
        const parsed = JSON.parse(line);

        switch (parsed.type) {
          case 'lruDisk':
            this.lruCacheDiskBacked.load(parsed.data);
            break;
          case 'lruFiltered':
            this.previouslyDereferenced?.load(parsed.data);
            break;
          case 'sizeMap':
            this.sizeMap.set(parsed.key, parsed.value);
            nDeserialized++;
            break;
          case 'savedMetadata':
            this.savedMetadata.set(parsed.key, parsed.value);
            break;
        }
      }

      console.log(
        `Successfully deserialized cache metadata with ${nDeserialized} entries ` +
        `in ${(performance.now() - startTime) / 1000} seconds.`,
      );
    } catch (error) {
      console.error('Failed to deserialize cache metadata:', error);
    }
  }

  public async clear(): Promise<void> {
    await this.readyPromise;

    // Wait for ingestion to finish to not leave ghost triples
    if (this.activeIngestions.size > 0) {
      await Promise.allSettled([ ...this.activeIngestions.values()]);
    }

    if (this.activeDeletions.size > 0) {
          await Promise.allSettled([ ...this.activeDeletions.values()]);
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
    const entries = Array.from(this.sizeMap.keys()).map((key): [string, ISourceState] => {
      const cacheGraph = this.getCacheGraphNode(key);
      return [ key, this.createSourceStateFromDisk(key, cacheGraph) ];
    });
    return new ArrayIterator(entries, { autoStart: false });
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

  protected triggerGraphDeletion(key: string): Promise<void> {
    if (this.activeDeletions.has(key)) {
      return this.activeDeletions.get(key)!;
    }

    const deletionPromise = new Promise<void>((resolve) => {
      const removalStream = this.store.deleteGraph(this.getCacheGraphNode(key));
      removalStream.on('end', resolve);
      removalStream.on('error', (err) => {
        console.warn(`Background graph deletion failed for ${key}:`, err);
        resolve(); 
      });
    }).finally(() => {
      this.activeDeletions.delete(key);
    });

    this.activeDeletions.set(key, deletionPromise);
    return deletionPromise;
  }
}

export interface IPersistentCacheIndexedDiskArgs {
  maxNumTriples: number;
  maxTriplesInMemory: number;
  serializationLoc?: string;
  hotCachePolicy: 'lru' | 'lru-filtered';
  metadataKeysToCache?: string[];
}
