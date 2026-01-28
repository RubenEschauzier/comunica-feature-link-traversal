import type {
  IActionContextPreprocess,
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { CacheEntrySourceState } from '@comunica/cache-manager-entries/lib';
import { CacheSourceStateViews } from '@comunica/cache-manager-entries/lib/ViewKeys';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type { ISourceState } from '@comunica/types';

import type { ICacheView, IPersistentCache, ISetFn } from '@comunica/types-link-traversal';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { PersistentCacheSourceStateNumTriples } from './PersistentCacheSourceStateNumTriples';

// TODO: Make the cache a seperate source, if cache is hit then the source gets updated and on update
// queryBindings will emit more bindings that match that document, instead of any reindexing?
/**
 * A comunica Set Defaults Traversal Caching Context Preprocess Actor.
 */
export class ActorContextPreprocessSetDefaultsTraversalCachingNumTriples extends ActorContextPreprocess {
  private readonly cacheSourceState: PersistentCacheSourceStateNumTriples;

  public constructor(args: IActorContextPreprocessSetSourceCacheNumTriplesArgs) {
    super(args);
    this.cacheSourceState = new PersistentCacheSourceStateNumTriples(
      { maxNumTriples: args.cacheSizeNumTriples },
    );
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
    cacheManager.registerCacheView(
      CacheSourceStateViews.cacheSourceStateView,
      new GetSourceStateCacheView(),
    );
    return { context };
  }
}

export class SetSourceStateCache implements ISetFn<ISourceState, ISourceState, { headers: Headers }> {
  protected DF: DataFactory = new DataFactory();
  protected AF: AlgebraFactory = new AlgebraFactory(this.DF);

  public async setInCache(
    key: string,
    value: ISourceState,
    cache: IPersistentCache<ISourceState>,
    context: { headers: Headers },
  ): Promise<void> {
    cache.set(key, value);
  }
}

export class GetSourceStateCacheView
implements ICacheView<ISourceState, { url: string }, ISourceState> {
  public async construct(cache: IPersistentCache<ISourceState>, context: { url: string }): Promise<ISourceState | undefined> {
    const cacheEntry = await cache.get(context.url);
    if (!cacheEntry) {
      return;
    }
    return cacheEntry;
  }
}

export interface IActorContextPreprocessSetSourceCacheNumTriplesArgs extends IActorContextPreprocessArgs {
  /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124_000}
   */
  cacheSizeNumTriples: number;
}
