import type * as RDF from '@rdfjs/types';
import { RdfStore } from 'rdf-stores';
import { termToString } from 'rdf-string';
import { ICacheMetrics, IPersistentCache } from '@comunica/types';
import { LRUCache } from 'lru-cache';
import { DataFactory } from 'rdf-data-factory';
import { Factory } from 'sparqlalgebrajs';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { AsyncIterator, ArrayIterator } from 'asynciterator';

// Assuming ISourceState has a source property and metadata like cachePolicy
export class UnifiedStoreCacheWrapper implements IPersistentCache<AsyncIterator<RDF.Quad>> {
  protected unifiedStore: UnifiedStoreCache;
  
  public readonly DF: DataFactory = new DataFactory();
  public readonly AF: Factory = new Factory(this.DF);
  public readonly BF: BindingsFactory = new BindingsFactory(this.DF, {});

  private isTracking: boolean = false;
  private cacheMetrics: ICacheMetrics;

  public constructor(unifiedStore: UnifiedStoreCache) {
    this.unifiedStore = unifiedStore;
    this.cacheMetrics = this.resetMetrics();
  }

  public async set(key: string, value: AsyncIterator<RDF.Quad>): Promise<void> {
    // 1. Drain the source stream and index the quads
    const count = await this.unifiedStore.importToCache(key, value);
    this.unifiedStore.setLruEntry(key, count);
  }

  public async get(key: string): Promise<AsyncIterator<RDF.Quad> | undefined> {
    // Return the metadata alongside a dummy source to satisfy the ISourceState interface.
    // The query engine will receive the actual data via the view's queryBindings/queryQuads interceptions.
    const quads = this.unifiedStore.getQuadsByUrl(key);

    if (this.isTracking) {
      quads ? this.cacheMetrics.hits++ : this.cacheMetrics.misses++;
    }
    
    if (quads){
      return new ArrayIterator(quads);
    }

    // TODO Implement some tracking of deleted size!
  }

  public async getMany(keys: string[]): Promise<(AsyncIterator<RDF.Quad> | undefined)[]> {
    return Promise.all(keys.map(key => this.get(key)));
  }

  public async has(key: string){
    return this.unifiedStore.hasEntry(key);
  }

  public entries(){
    return new ArrayIterator(
      this.unifiedStore.cacheEntries(),
      { autoStart: false },
    );
  }

  public async delete(key: string): Promise<boolean> {
    return this.unifiedStore.deleteFromCache(key);
  }

  public async size(){
    return this.unifiedStore.size();
  }

  public serialize(): Promise<void> {
    throw new Error('Serialize implemented for this in-memory cache');
  }

  public startSession(){
    this.isTracking = true;
    this.cacheMetrics = this.resetMetrics();
    return this.cacheMetrics;
  }

  public endSession(){
    this.isTracking = false;
    return this.cacheMetrics;
  }

  public resetMetrics(): ICacheMetrics{
    return {
      hits: 0,
      misses: 0,
      evictions: 0,
      evictionsCalculatedSize: 0,
      evictionPercentage: 0,
    }
  }

}

export class UnifiedStoreCache {
  protected store: RdfStore = RdfStore.createDefault();
  protected urlToQuads: Map<string, RDF.Quad[]> = new Map();

  // Maps a serialized quad string to a Set of origin URLs
  protected serializedQuadToUrls: Map<string, Set<string>> = new Map();
  protected lru: LRUCache<string, string>;

  public constructor(maxNumTriples: number) {
    this.lru = new LRUCache<string, string>({
      max: maxNumTriples,
      sizeCalculation: () => 1,
      // The dispose callback guarantees the store cleans up upon URL eviction
      dispose: (value: string, key: string) => {
        this.evictFromStore(key);
      }
    });
  }

  protected serializeQuad(quad: RDF.Quad): string {
    return `${termToString(quad.subject)}|${termToString(quad.predicate)}|${termToString(quad.object)}|${termToString(quad.graph)}`;
  }

  protected evictFromStore(url: string): void {
    const quads = this.urlToQuads.get(url);
    if (quads) {
      for (const quad of quads) {
        const quadId = this.serializeQuad(quad);
        const urlSet = this.serializedQuadToUrls.get(quadId);
        
        if (urlSet) {
          urlSet.delete(url);
          // If no other URLs assert this quad, remove it entirely from the store
          if (urlSet.size === 0) {
            this.serializedQuadToUrls.delete(quadId);
            this.store.removeQuad(quad);
          }
        }
      }
      this.urlToQuads.delete(url);
    }
  }

  /**
   * Import the data into the streaming cache
   * @param url 
   * @param quads 
   * @returns 
   */
  public importToCache(url: string, quads: AsyncIterator<RDF.Quad>): Promise<number>{
    return new Promise((resolve, reject) => {
      const quadsFromSource: RDF.Quad[] = [];

      const quadsToImport = quads.map((quad) => {
        quadsFromSource.push(quad);
        const quadId = this.serializeQuad(quad);
        
        let urlSet = this.serializedQuadToUrls.get(quadId);
        if (!urlSet) {
          urlSet = new Set();
          this.serializedQuadToUrls.set(quadId, urlSet);
        }
        urlSet.add(url);
        
        return quad;
      });

      const emitter = this.store.import(quadsToImport);

      emitter.on('end', () => {
        // Register the full array for future cache eviction operations
        this.urlToQuads.set(url, quadsFromSource);
        
        // Resolve the promise with the final count
        resolve(quadsFromSource.length);
      });

      emitter.on('error', (error) => {
        reject(error);
      });
    });
  }

  /**
   * After importing the quads we set the lru cache with the size in triples
   * @param url 
   * @param size 
   */
  public setLruEntry(url: string, size: number){
    this.lru.set(url, url, { size });
  }

  public hasEntry(url: string): boolean{
    return this.lru.has(url);
  }

  public cacheEntries(): [string, AsyncIterator<RDF.Quad>][] {
    return [...this.lru.keys()].map(key => [key, new ArrayIterator(this.getQuadsByUrl(key)!)]);
  }

  public getQuadsByUrl(url: string): RDF.Quad[] | undefined {
    if (this.lru.has(url)) {
      this.lru.get(url);
      return this.urlToQuads.get(url);
    }
    return undefined;
  }

  public deleteFromCache(url: string): boolean {
    return this.lru.delete(url);
  }
  
  public matchReleased(
    subject: RDF.Term | undefined,
    predicate: RDF.Term | undefined,
    object: RDF.Term | undefined,
    graph: RDF.Term | undefined,
    releasedUrls: Set<string>
  ): AsyncIterator<RDF.Quad> {
    const matches = this.store.match(subject, predicate, object, graph);
    
    return matches.filter(quad => {
      const quadId = this.serializeQuad(quad);
      const urlSet = this.serializedQuadToUrls.get(quadId);
      
      if (!urlSet) return false;
      
      // Keep the quad if at least one of its origin URLs has been released
      for (const url of urlSet) {
        if (releasedUrls.has(url)) return true;
      }
      return false;
    });
  }

  public size(){
    return this.lru.calculatedSize;
  }
}