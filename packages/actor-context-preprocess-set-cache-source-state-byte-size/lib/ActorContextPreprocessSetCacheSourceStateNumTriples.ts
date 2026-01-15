import type { ICacheView, ISetFn, ISourceState } from '@comunica/actor-context-preprocess-set-persistent-cache-manager';
import type {
  IActionContextPreprocess,
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { CacheEntrySourceState } from '@comunica/cache-manager-entries/lib';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';

// TODO THIS SHOULD BE A @comunica/TYPES IMPORT
import { LRUCache } from 'lru-cache';

/**
 * A comunica Set Defaults Traversal Caching Context Preprocess Actor.
 */
export class ActorContextPreprocessSetDefaultsTraversalCachingNumTriples extends ActorContextPreprocess {
  private readonly cacheSourceState: LRUCache<string, ISourceState>;

  public constructor(args: IActorContextPreprocessSetSourceCacheNumTriplesArgs) {
    super(args);
    this.cacheSourceState = new LRUCache<string, ISourceState>({
      maxSize: args.cacheSizeStore,
      sizeCalculation: ActorContextPreprocessSetDefaultsTraversalCachingNumTriples.getSizeSource,
    });
  }

  public async test(_action: IAction): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    const context = action.context;
    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceState,
      this.cacheSourceState,
      new SetSourceStateCache(),
    );
    return { context };
  }

  /**
   * Get number of triples in store as size
   * @param source
   * @returns Size of source in number of triples
   */
  protected static getSizeSource(source: ISourceState, _key: string): number {
    const nTriples = (<any>source.source).source._size;
    return nTriples;
  }
}

export class SetSourceStateCache implements ISetFn<ISourceState, Record<string, any>> {
  public setInCache(key: string, value: ISourceState, cache: any, _context: Record<string, any>): void {
    cache.set(key, value);
  }
}
export class GetSourceStateCacheView implements ICacheView<ISourceState, { url: string }> {
  public construct(cache: any, context: { url: string }): ISourceState {
    return cache.get(context.url);
  }
}

export interface IActorContextPreprocessSetSourceCacheNumTriplesArgs extends IActorContextPreprocessArgs {
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
