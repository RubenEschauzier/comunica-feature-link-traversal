import { ActorContextPreprocess, IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries';
import { TestResult, IActorTest, passTestVoid, ActionContext } from '@comunica/core';
import { ICacheView, IPersistentCache, ISourceState } from '@comunica/types';
import { Algebra, isKnownOperation } from '@comunica/utils-algebra';

/**
 * A comunica Set Cache Count View Context Preprocess Actor.
 */
export class ActorContextPreprocessSetCacheCountView extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(_action: IActionContextPreprocess): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    const context = action.context;
    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCacheView(
      CacheSourceStateViews.cacheCountView,
      new CacheCountView(),
    );
    return { context };
  }
}

export class CacheCountView
implements ICacheView<ISourceState, { operation: Algebra.Operation, documents: string[] }, number> {
  public async construct(
    cache: IPersistentCache<ISourceState>,
    context: { operation: Algebra.Operation, documents: string[] }
  ): Promise<number | undefined> {
    if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
      let totalCount = 0;
      const cacheEntryStream = cache.entries();

      for await (const [key, source] of cacheEntryStream) {
        if (source.source.countQuads) {
          // Safely await the result
          const quadCount = await source.source.countQuads(context.operation, new ActionContext());
          totalCount += quadCount;
        }
      }
      return totalCount;
    }
    throw new Error("Count view only accepts quad patterns");
  }
}