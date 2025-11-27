import type {
  IActionContextPreprocess,
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysCaches } from '@comunica/context-entries';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { passTestVoid, ActionContextKey } from '@comunica/core';
import type { ICacheStatistics, ISourceState } from '@comunica/types';

// eslint-disable-next-line ts/no-require-imports
import CachePolicy = require('http-cache-semantics');
import { LRUCache } from 'lru-cache';
import { DataFactory } from 'rdf-data-factory';

/**
 * A comunica Set Defaults Traversal Caching Context Preprocess Actor.
 */
export class ActorContextPreprocessSetDefaultsTraversalCachingByteSize extends ActorContextPreprocess {
  private readonly policyCache: LRUCache<string, CachePolicy>;
  private readonly storeCache: LRUCache<string, ISourceState>;
  private readonly cacheSizePolicy: number;
  private readonly cacheSizeStore: number;
  private cacheStatistics: ICacheStatistics;
  private readonly DF: DataFactory = new DataFactory();

  public constructor(args: IActorContextPreprocessSetSourceCacheByteSizeArgs) {
    super(args);
    this.policyCache = new LRUCache<string, CachePolicy>({ max: this.cacheSizePolicy });
    this.storeCache = new LRUCache<string, ISourceState>({
      maxSize: this.cacheSizeStore,
      sizeCalculation: ActorContextPreprocessSetDefaultsTraversalCachingByteSize.getSizeSource,
      dispose: this.dispose.bind(this),
    });
  }

  public async test(_action: IAction): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    let context = action.context;
    if (context.get(KeysCaches.cleanCache) || context.get(new ActionContextKey('cleanCache'))) {
      this.policyCache.clear();
      this.storeCache.clear();
    }
    this.cacheStatistics = context.getSafe(KeysCaches.cacheStatistics);

    context = context
      .setDefault(KeysCaches.policyCache, this.policyCache)
      .setDefault(KeysCaches.storeCache, this.storeCache);
    return { context };
  }

  /**
   * Get number of triples in store as size
   * @param source
   * @returns
   */
  private static getSizeSource(source: ISourceState, _key: string): number {
    const nTriples = (<any>source.source).source._size;
    if (nTriples === 0) {
      return 1;
    }
    return nTriples;
  }

  /**
   * Policies are disposed when the associated store is evicted
   * @param value
   * @param key
   * @param reason
   */
  private dispose(value: ISourceState, key: string, reason: string): void {
    if (reason === 'evict' || reason === 'delete') {
      console.log(this.cacheStatistics)
      this.cacheStatistics.evictions++;
      this.cacheStatistics.evictionsTriples +=
       ActorContextPreprocessSetDefaultsTraversalCachingByteSize.getSizeSource(value, key);
      this.cacheStatistics.evictionPercentage =
        this.cacheStatistics.evictionsTriples / this.cacheSizeStore;
      this.policyCache.delete(key);
    }
  }
}

export interface IActorContextPreprocessSetSourceCacheByteSizeArgs extends IActorContextPreprocessArgs {
  /**
   * The maximum number of entries in the source cache, should be high, as evictions in the
   * store cache will also evict in the policy cache.
   * @range {integer}
   * @default {10000}
   */
  cacheSizePolicy: number;
  /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124_000}
   */
  cacheSizeStore: number;
}
