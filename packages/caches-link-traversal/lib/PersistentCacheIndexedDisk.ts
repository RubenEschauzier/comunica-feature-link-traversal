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
import { ArrayIterator, TransformIterator, wrap } from 'asynciterator';
import { ClassicLevel } from 'classic-level';
import { LRUCache } from 'lru-cache';
import { Quadstore } from 'quadstore';
import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'node:stream';

let globalDbInstance: ClassicLevel | null = null;
let globalStoreInstance: Quadstore | null = null;
let activeConnectionCount = 0;

function getSharedQuadstore(serializationLoc: string, dataFactory: any): { store: Quadstore; db: ClassicLevel; readyPromise: Promise<void> } {
  activeConnectionCount++;

  if (!globalStoreInstance) {
    globalDbInstance = new ClassicLevel(serializationLoc, {
      writeBufferSize: 4 * 1024 * 1024, 
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
      db: globalDbInstance,
      readyPromise: globalStoreInstance.open().catch((err) => {
        console.error('FATAL: Failed to open persistent quadstore cache:', err);
        process.exit(1);
      }),
    };
  }

  return {
    store: globalStoreInstance,
    db: globalDbInstance!,
    readyPromise: Promise.resolve(),
  };
}

export class PersistentCacheIndexedDisk implements IPersistentCache<ISourceState, ISourceState> {
  private readonly maxNumTriplesDisk: number;
  private readonly maxNumTriplesInMemory: number;
  private readonly activeIngestions = new Map<string, Promise<void>>();
  private readonly activeDeletions = new Map<string, Promise<void>>();

  private readonly hotLRUCacheDocuments: LRUCache<string, ISourceState>;
  private readonly hotCachePolicy: 'lru' | 'lru-filtered';
  private readonly previouslyDereferenced?: LRUCache<string, number>;
  private readonly decayThreshold = 10000;
  private nAccesses = 0;

  private readonly lruCacheDiskBacked: LRUCache<string, boolean>;
  private readonly metadataKeysToCache: string[];
  private readonly sizeMap = new Map<string, number>();

  private readonly dataFactory = new DataFactory();
  private readonly bindingsFactory = new BindingsFactory(this.dataFactory);
  private readonly algebraFactory = new AlgebraFactory(this.dataFactory);

  // Prevent massive number of deleteQuads / ingestQuads operations 
  // from overwhelming the disk I/O
  private readonly dbQueue = new ConcurrencyQueue(5);

  private isTracking = false;
  private cacheMetrics: ICacheMetrics;

  private readonly serializationLoc: string;
  private isClosed: boolean;
  private store: Quadstore;
  private db: ClassicLevel;
  private readyPromise: Promise<void>;
  
  public constructor(args: IPersistentCacheIndexedDiskArgs) {
    this.maxNumTriplesDisk = args.maxNumTriples;
    this.maxNumTriplesInMemory = args.maxTriplesInMemory;
    this.serializationLoc = args.serializationLoc ?? `${__dirname}/../cache/`;

    const connection = getSharedQuadstore(this.serializationLoc, this.dataFactory);
    this.store = connection.store;
    this.db = connection.db;
    this.readyPromise = connection.readyPromise;
    this.isClosed = false;

    this.lruCacheDiskBacked = new LRUCache<string, boolean>({
      maxSize: this.maxNumTriplesDisk,
      sizeCalculation: (value, key) => this.sizeMap.get(key) || 1,
      dispose: this.onDispose.bind(this),
      noDisposeOnSet: true,
    });

    this.hotLRUCacheDocuments = new LRUCache<string, ISourceState>({
      maxSize: this.maxNumTriplesInMemory,
      sizeCalculation: (value, key) => this.sizeMap.get(key) || 1,
      dispose: this.onDisposeHotCache.bind(this),
      noDisposeOnSet: true,
    });

    this.hotCachePolicy = args.hotCachePolicy;
    if (this.hotCachePolicy === 'lru-filtered') {
      this.previouslyDereferenced = new LRUCache<string, number>({ max: 50000 });
    }
    this.metadataKeysToCache = args.metadataKeysToCache ?? [ 'traverse', 'defaultTraversal', 'predicateToLinks' ];

    this.cacheMetrics = this.resetMetrics();

    const cleanup = () => {
      this.store.close().catch(() => {});
    };
    process.on('exit', cleanup);
    process.on('SIGINT', () => { cleanup(); process.exit(2); });
    process.on('SIGTERM', () => { cleanup(); process.exit(15); });
  }

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

    return visits >= 2;
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
      if (this.isTracking) this.cacheMetrics.additionalMetrics!.hotCache.hits++;
      return cachedState;
    }

    let storedMetaPayload: any;
    try {
      const metaStr = await this.db.get(`meta:${key}`);
      storedMetaPayload = JSON.parse(metaStr!);
    } catch (err) {
      if (this.isTracking) this.cacheMetrics.misses++;
      return undefined;
    }

    if (this.isTracking) this.cacheMetrics.hits++;

    this.lruCacheDiskBacked.get(key);

    const cacheGraph = this.getCacheGraphNode(key);
    const rehydratedState = this.createSourceStateFromDisk(key, cacheGraph, storedMetaPayload.data);

    if (this.canAddToHotCache(key)) {
      this.hotLRUCacheDocuments.set(key, rehydratedState);
    }

    return rehydratedState;
  }

  public async set(key: string, value: ISourceState): Promise<void> {
    if (this.isClosed) return;
    await this.readyPromise;

    if (this.lruCacheDiskBacked.has(key)) {
      this.triggerGraphDeletion(key);
    }

    const ongoingDeletion = this.activeDeletions.get(key);
    if (ongoingDeletion) await ongoingDeletion;

    if (this.activeIngestions.has(key)) return this.activeIngestions.get(key);

    const ingestionPromise = this.dbQueue.run(() => this._set(key, value))
      .finally(() => {
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

    const metadataPayload = {
      size: nTriples,
      ingestedAt: Date.now(),
      data: extractedMetadata
    };

    await this.db.put(`meta:${key}`, JSON.stringify(metadataPayload));

    this.sizeMap.set(key, nTriples);
    this.lruCacheDiskBacked.set(key, true);

    const rehydratedState = this.createSourceStateFromDisk(key, cacheGraph, extractedMetadata);
    if (this.canAddToHotCache(key)) {
      this.hotLRUCacheDocuments.set(key, rehydratedState);
    }
  }

  private createSourceStateFromDisk(key: string, cacheGraph: RDF.NamedNode, metadataFromDisk: any): ISourceState {
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
        this.store.countQuads(
          sanitizeTerm(subject),
          sanitizeTerm(predicate),
          sanitizeTerm(object),
          cacheGraph,
        ),
    };

    return {
      link: { url: key },
      metadata: {
        traverse: [],
        state: new MetadataValidationState(),
        cardinality: { value: 0, type: 'estimate' },
        variables: [],
        ...metadataFromDisk
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
    if (reason === 'set') {
      return;
    }
    
    if (reason === 'evict' && this.isTracking) {
      this.cacheMetrics.evictions++;
      this.cacheMetrics.evictionsCalculatedSize += this.sizeMap.get(key) ?? 1;
      this.cacheMetrics.evictionPercentage =
        (this.cacheMetrics.evictionsCalculatedSize / this.maxNumTriplesDisk) * 100;
    }

    this.sizeMap.delete(key);
    this.hotLRUCacheDocuments.delete(key);

    this.triggerGraphDeletion(key);
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
    if (this.hotLRUCacheDocuments.has(key)) return true;

    try {
      await this.db.get(`meta:${key}`);
      return true;
    } catch {
      return false;
    }
  }

  public async delete(key: string): Promise<boolean> {
    await this.readyPromise;

    if (this.activeIngestions.has(key)) {
      try { await this.activeIngestions.get(key); } catch {}
    }

    const existed = this.lruCacheDiskBacked.has(key);
    if (!existed) return false;

    this.lruCacheDiskBacked.delete(key);
    await this.triggerGraphDeletion(key);
    return true;
  }

  public async serialize(): Promise<void> {
    const startTime = performance.now();
    this.isClosed = true;
    await this.readyPromise;
    if (this.activeIngestions.size > 0) {
      console.log(`Waiting for ${this.activeIngestions.size} active ingestions to complete before serialization...`);
      await Promise.allSettled([...this.activeIngestions.values()]);
    }
    console.log(`Cache serialization completed in ${(performance.now() - startTime) / 1000} seconds.`);
  }
  
  public async deserialize(): Promise<void> {
    await this.readyPromise;
    
    this.sizeMap.clear();
    this.lruCacheDiskBacked.clear();

    const startTime = performance.now();
    const entries: { key: string; size: number; ingestedAt: number }[] = [];

    try {
      const iterator = this.db.iterator({ gte: 'meta:', lte: 'meta:\xFF' });
      for await (const [dbKey, dbValue] of iterator) {
        try {
          const key = dbKey.slice(5); 
          const payload = JSON.parse(dbValue);
          entries.push({ key, size: payload.size || 1, ingestedAt: payload.ingestedAt || 0 });
        } catch (err) {
          console.warn(`Corrupted metadata payload found for key ${dbKey}`);
        }
      }

      entries.sort((a, b) => a.ingestedAt - b.ingestedAt);

      for (const entry of entries) {
        this.sizeMap.set(entry.key, entry.size);
        this.lruCacheDiskBacked.set(entry.key, true); 
      }

      console.log(`Successfully restored cache memory state with ${entries.length} entries in ${(performance.now() - startTime) / 1000} seconds.`);
      console.log(`Current cache size: ${this.sizeMap.size} entries, total triples: ${[...this.sizeMap.values()].reduce((sum, size) => sum + size, 0)}`);
      let count = 0;
      const quadStream = this.store.match();

      for await (const _quad of quadStream) {
        count++;
      }      
      console.log(`Current quadstore size: ${count} triples`);
    } catch (error) {
      console.error('Failed to rebuild cache metadata from LevelDB:', error);
    }
  }

  public async clear(): Promise<void> {
    await this.readyPromise;

    if (this.activeIngestions.size > 0) await Promise.allSettled([ ...this.activeIngestions.values()]);
    if (this.activeDeletions.size > 0) await Promise.allSettled([ ...this.activeDeletions.values()]);

    this.sizeMap.clear();
    this.hotLRUCacheDocuments.clear();
    this.lruCacheDiskBacked.clear();
    this.activeIngestions.clear();

    if (globalStoreInstance) {
      await globalStoreInstance.close();
      globalStoreInstance = null;
      globalDbInstance = null;
    }

    await ClassicLevel.destroy(this.serializationLoc);

    const connection = getSharedQuadstore(this.serializationLoc, this.dataFactory);
    this.store = connection.store;
    this.db = connection.db;
    this.readyPromise = connection.readyPromise;

    await this.readyPromise;
  }

  public entries(): AsyncIterator<[string, ISourceState]> {
    const self = this;

    async function* generateEntries(): AsyncGenerator<[string, ISourceState]> {
      await self.readyPromise;

      const keys = [ ...self.lruCacheDiskBacked.keys() ];

      for (const key of keys) {
        const value = await self.get(key);
        if (value) {
          yield [ key, value ];
        }
      }
    }

    const nodeStream = Readable.from(generateEntries(), { objectMode: true });
    return wrap(nodeStream);
  }

  public async size(): Promise<number> {
    await this.readyPromise;
    return await this.store.countQuads();
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
        hotCache: { hits: 0, misses: 0, evictions: 0, evictionsCalculatedSize: 0, evictionPercentage: 0 },
      },
    };
  }

  protected triggerGraphDeletion(key: string): Promise<void> {
    if (this.activeDeletions.has(key)) return this.activeDeletions.get(key)!;

    const deletionPromise = this.dbQueue.run(async () => {
      try {
        await this.db.del(`meta:${key}`);
      } catch (err) {
        // Continue if deletion fails or key is already missing
      }

      return new Promise<void>((resolve) => {
        const removalStream = this.store.deleteGraph(this.getCacheGraphNode(key));
        
        removalStream.on('end', resolve);
        removalStream.on('error', (err) => {
          console.warn(`Background graph deletion failed for ${key}:`, err);
          resolve(); 
        });
      });
    }).finally(() => {
      this.activeDeletions.delete(key);
    });

    this.activeDeletions.set(key, deletionPromise);
    return deletionPromise;
  }
}

class ConcurrencyQueue {
  private activeCount = 0;
  private queue: (() => void)[] = [];

  constructor(private concurrencyLimit: number) {}

  public async run<T>(task: () => Promise<T>): Promise<T> {
    if (this.activeCount >= this.concurrencyLimit) {
      await new Promise<void>((resolve) => this.queue.push(resolve));
    }
    this.activeCount++;
    try {
      return await task();
    } finally {
      this.activeCount--;
      if (this.queue.length > 0) {
        const next = this.queue.shift();
        next!();
      }
    }
  }
}

export interface IPersistentCacheIndexedDiskArgs {
  maxNumTriples: number;
  maxTriplesInMemory: number;
  serializationLoc?: string;
  hotCachePolicy: 'lru' | 'lru-filtered';
  metadataKeysToCache?: string[];
}