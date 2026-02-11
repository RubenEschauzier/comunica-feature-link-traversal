import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';
import { ActionContext } from '@comunica/core';
import type { ISourceStateBloomFilter } from '@comunica/types';
import type { ICacheMetrics, IPersistentCache } from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator } from 'asynciterator';
import { LRUCache } from 'lru-cache';
import { DataFactory } from 'rdf-data-factory';
import { RdfStore } from 'rdf-stores';
import { Factory } from 'sparqlalgebrajs';
import { BloomFilter } from 'bloom-filters';
import type * as RDF from '@rdfjs/types';


export class PersistentCacheSourceStateIndexedBloomFilter implements IPersistentCache<ISourceStateBloomFilter> {
  private readonly sizeMap = new Map<string, number>();
  private readonly maxNumTriples: number;
  private readonly lruCacheDocuments: LRUCache<string, ISourceStateBloomFilter>;

  public readonly DF: DataFactory = new DataFactory();
  public readonly AF: Factory = new Factory(this.DF);
  public readonly BF: BindingsFactory = new BindingsFactory(this.DF, {});

  private isTracking: boolean = false;
  private cacheMetrics: ICacheMetrics;

  public constructor(args: IPersistentCacheSourceStateNumTriplesArgs) {
    this.maxNumTriples = args.maxNumTriples;
    this.lruCacheDocuments = new LRUCache<string, ISourceStateBloomFilter>({
      maxSize: this.maxNumTriples,
      sizeCalculation: (value, key) => this.sizeMap.get(key) || 1,
      dispose: this.onDispose.bind(this),
    });
    this.cacheMetrics = this.resetMetrics();
  }

  public async get(key: string): Promise<ISourceStateBloomFilter | undefined> {
    return this.getSync(key);
  }

  public getSync(key: string): ISourceStateBloomFilter | undefined{
    const cachedState = this.lruCacheDocuments.get(key);

    if (this.isTracking) {
      cachedState ? this.cacheMetrics.hits++ : this.cacheMetrics.misses++;
    }

    return cachedState;
  }

  public async getMany(keys: string[]): Promise<(ISourceStateBloomFilter | undefined)[]> {
    return keys.map(key => this.getSync(key));
  }

  /**
   * Upon setting of a source, we index it and set it in the LRUCache.
   * @param key 
   * @param value 
   * @returns 
   */
  public async set(key: string, value: ISourceStateBloomFilter): Promise<void> {
    const bloomFilter = new BloomFilterOwn();
    const rdfStore = RdfStore.createDefault();
    const importStream = rdfStore.import(value.source.queryQuads(
      this.AF.createPattern(
        this.DF.variable('s'),
        this.DF.variable('p'),
        this.DF.variable('o'),
        this.DF.variable('g'),
      ),
      new ActionContext(),
    ));
    importStream.on('data', (data: RDF.Quad) => {
      bloomFilter.add(data.subject.value);
      bloomFilter.add(data.object.value);
    });
    return new Promise((resolve, reject) => {
      importStream.on('end', () => {
        this.sizeMap.set(key, rdfStore.size);
        this.lruCacheDocuments.set(key, 
          { 
            ...value,
            source: new QuerySourceRdfJs(
              rdfStore,
              this.DF,
              this.BF
            ),
            bloomFilter: <any> bloomFilter
          }
        )
        resolve()
      });
      importStream.on('error', () => {
        reject('Import stream error')
      });
    })
  }


  protected onDispose(value: ISourceStateBloomFilter, key: string, reason: LRUCache.DisposeReason): void {
    if (reason === 'evict' && this.isTracking){
      this.cacheMetrics.evictions++;
      this.cacheMetrics.evictionsCalculatedSize += this.sizeMap.get(key) ?? 1;
      this.cacheMetrics.evictionPercentage = 
        (this.cacheMetrics.evictionsCalculatedSize / this.maxNumTriples)*100;
      if (this.sizeMap.has(key)) {
        this.sizeMap.delete(key);
      }
    }
  }

  public async has(key: string): Promise<boolean> {
    return this.lruCacheDocuments.has(key);
  }

  public async delete(key: string): Promise<boolean> {
    return this.lruCacheDocuments.delete(key);
  }

  public entries(): AsyncIterator<[string, ISourceStateBloomFilter]> {
    return new ArrayIterator(
      this.lruCacheDocuments.entries(),
      { autoStart: false },
    );
  }

  public async size(): Promise<number> {
    return this.lruCacheDocuments.calculatedSize;
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

export interface IPersistentCacheSourceStateNumTriplesArgs {
  maxNumTriples: number;
}


import MurmurHash3 from 'imurmurhash';

export class BloomFilterOwn {
  private bitArray: Uint32Array;
  private sizeMask: number; // Used for bitwise wrapping instead of modulo
  private hashCount: number;

  constructor(size: number = 8192, hashCount: number = 11) {
    this.sizeMask = size - 1;
    this.hashCount = hashCount
    this.bitArray = new Uint32Array(Math.ceil(size / 32));
  }

  public add(key: string): void {
    // Inline hashing to avoid object allocation overhead
    // Seed 1
    const h1 = MurmurHash3(key, 0).result();
    // Seed 2 (0x5bd1e995 is a common mixing constant)
    const h2 = MurmurHash3(key, 0x5bd1e995).result();

    for (let i = 0; i < this.hashCount; i++) {
      // OPTIMIZATION: Bitwise AND (&) is faster than Modulo (%)
      // This works because size is guaranteed to be a power of 2
      const index = (h1 + i * h2) & this.sizeMask;
      
      // index >>> 5 is equivalent to Math.floor(index / 32)
      // index & 31 is equivalent to index % 32
      this.bitArray[index >>> 5] |= (1 << (index & 31));
    }
  }

  public has(key: string): boolean {
    const h1 = MurmurHash3(key, 0).result();
    const h2 = MurmurHash3(key, 0x5bd1e995).result();

    for (let i = 0; i < this.hashCount; i++) {
      const index = (h1 + i * h2) & this.sizeMask;
      
      if ((this.bitArray[index >>> 5] & (1 << (index & 31))) === 0) {
        return false;
      }
    }
    return true;
  }

}