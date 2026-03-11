import type {
  IActionContextPreprocess,
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { CacheEntrySourceState } from '@comunica/cache-manager-entries/lib';
import { CacheSourceStateViews } from '@comunica/cache-manager-entries/lib/ViewKeys';
import { KeysCaching } from '@comunica/context-entries';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { ActionContextKey, passTestVoid } from '@comunica/core';
import type { ISourceState, ICacheView, IPersistentCache, ISetFn } from '@comunica/types';

import { AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { PersistentCacheSourceStateNumTriples } from './PersistentCacheSourceStateNumTriples';

/**
 * A comunica Set Defaults Traversal Caching Context Preprocess Actor.
 */
export class ActorContextPreprocessSetCacheSourceState extends ActorContextPreprocess {
  private readonly cacheSizeNumTriples: number;
  private cacheSourceState: PersistentCacheSourceStateNumTriples;

  public constructor(args: IActorContextPreprocessSetSourceCacheNumTriplesArgs) {
    super(args);
    this.cacheSizeNumTriples = args.cacheSizeNumTriples;
    console.log(`Maximum cache size: ${args.cacheSizeNumTriples}`);
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

    // TEMP Solution due to my own sparql benchmark runner adjustments
    if (context.get(KeysCaching.clearCache) || context.get(new ActionContextKey('clearCache'))) {
      console.log(`Cleaned cache.`);
      this.cacheSourceState = new PersistentCacheSourceStateNumTriples(
        { maxNumTriples: this.cacheSizeNumTriples },
      );
    }
    console.log(`Cache size: ${await this.cacheSourceState.size()}`);

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
   * @default {124000}
   */
  cacheSizeNumTriples: number;
}
