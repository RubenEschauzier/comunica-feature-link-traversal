import type { ISourceState } from '@comunica/types';
import type { ICacheMetrics, IPersistentCache } from '@comunica/types';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator } from 'asynciterator';
import { LRUCache } from 'lru-cache';

export class PersistentCacheSourceStateNumTriples implements IPersistentCache<ISourceState> {
  private readonly sizeMap = new Map<string, number>();
  private readonly maxNumTriples: number;
  private readonly lruCacheDocuments: LRUCache<string, ISourceState>;
  private isTracking: boolean = false;
  private cacheMetrics: ICacheMetrics


  public constructor(args: IPersistentCacheSourceStateNumTriplesArgs) {
    this.maxNumTriples = args.maxNumTriples;
    this.lruCacheDocuments = new LRUCache<string, ISourceState>({
      maxSize: this.maxNumTriples,
      sizeCalculation: (value, key) => this.sizeMap.get(key) || 1,
      dispose: this.onDispose.bind(this),
    });
    this.cacheMetrics = this.resetMetrics();
  }

  public async get(key: string): Promise<ISourceState | undefined> {
    return this.getSync(key);
  }

  public getSync(key: string): ISourceState | undefined{
    const cachedState = this.lruCacheDocuments.get(key);

    if (this.isTracking) {
      cachedState ? this.cacheMetrics.hits++ : this.cacheMetrics.misses++;
    }

    return cachedState;
  }

  public async getMany(keys: string[]): Promise<(ISourceState | undefined)[]> {
    return keys.map(key => this.getSync(key));
  }

  public async set(key: string, value: ISourceState): Promise<void> {
    this.lruCacheDocuments.set(key, value);
    if ('getSize' in value.source &&
            typeof value.source.getSize === 'function') {
      (<Promise<number>>value.source.getSize()).then((finalSize) => {
        if (this.lruCacheDocuments.has(key)) {
          this.sizeMap.set(key, finalSize);
          // We have to explicitly delete as .set() reuses the previous computed size
          this.lruCacheDocuments.delete(key);
          // Re-setting the key updates its size in the LRU engine
          this.lruCacheDocuments.set(key, value);
        }
      }).catch(() => {
        // Ignore stream errors here; they are handled by the main query consumer.
      });
    }
  }

  protected onDispose(value: ISourceState, key: string, reason: LRUCache.DisposeReason): void {
    if (reason === 'evict'){
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

  public entries(): AsyncIterator<[string, ISourceState]> {
    return new ArrayIterator(
      [ ...this.lruCacheDocuments.entries() ],
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
