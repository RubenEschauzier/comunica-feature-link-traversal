import type { ISourceState } from '@comunica/types';
import type { IPersistentCache } from '@comunica/types-link-traversal';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator } from 'asynciterator';
import { LRUCache } from 'lru-cache';

export class PersistentCacheSourceStateNumTriples implements IPersistentCache<ISourceState> {
  private readonly sizeMap = new Map<string, number>();
  private readonly lruCacheDocuments: LRUCache<string, ISourceState>;

  public constructor(args: IPersistentCacheSourceStateNumTriplesArgs) {
    this.lruCacheDocuments = new LRUCache<string, ISourceState>({
      maxSize: args.maxNumTriples,
      sizeCalculation: (value, key) => this.sizeMap.get(key) || 1,
    });
  }

  public async get(key: string): Promise<ISourceState | undefined> {
    return this.lruCacheDocuments.get(key);
  }

  public async getMany(keys: string[]): Promise<(ISourceState | undefined)[]> {
    return keys.map(key => this.lruCacheDocuments.get(key));
  }

  public async set(key: string, value: ISourceState): Promise<void> {
    this.lruCacheDocuments.set(key, value);
    // if ('getSize' in value.source &&
    //         typeof value.source.getSize === 'function') {
    //   (<Promise<number>>value.source.getSize()).then((finalSize) => {
    //     if (this.lruCacheDocuments.has(key)) {
    //       this.sizeMap.set(key, finalSize);
    //       // Re-setting the key updates its size in the LRU engine
    //       this.lruCacheDocuments.set(key, value);
    //     }
    //   }).catch(() => {
    //     // Ignore stream errors here; they are handled by the main query consumer.
    //   });
    // }
  }

  public async has(key: string): Promise<boolean> {
    return this.lruCacheDocuments.has(key);
  }

  public async delete(key: string): Promise<boolean> {
    return this.lruCacheDocuments.delete(key);
  }

  public entries(): AsyncIterator<[string, ISourceState]> {
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
}

export interface IPersistentCacheSourceStateNumTriplesArgs {
  maxNumTriples: number;
}
