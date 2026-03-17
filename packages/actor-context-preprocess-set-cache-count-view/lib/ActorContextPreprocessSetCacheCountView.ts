import type { PersistentCacheManager } from '@comunica/actor-context-preprocess-set-persistent-cache-manager';
import type { IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries';
import type { TestResult, IActorTest } from '@comunica/core';
import { passTestVoid, ActionContext } from '@comunica/core';
import type { ICacheView, IPersistentCache, ISourceState } from '@comunica/types';
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
    const cacheManager: PersistentCacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCacheView(
      CacheSourceStateViews.cacheCountView,
      new CacheCountView(),
    );
    return { context };
  }
}

export class CacheCountView
implements ICacheView<ISourceState, { operation: Algebra.Operation; documents: string[] }, number> {
  protected readonly computedCounts: Record<string, number> = {};

  public async construct(
    cache: IPersistentCache<ISourceState>,
    context: { operation: Algebra.Operation; documents: string[] },
  ): Promise<number | undefined> {
    if (!isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
      throw new Error('Count view only accepts quad patterns');
    }

    const pattern = context.operation;
    const patternKey = this.patternKey(pattern);

    if (patternKey in this.computedCounts) {
      return this.computedCounts[patternKey];
    }
    
    let totalCount = 0;
    const cacheEntryStream = cache.entries();

    for await (const [ key, source ] of cacheEntryStream) {
      if (source.source.countQuads) {
        const quadCount = await source.source.countQuads(context.operation, new ActionContext());
        totalCount += quadCount;
      }
    }
    this.computedCounts[patternKey] = totalCount;
    return totalCount;
  }

  private patternKey(pattern: Algebra.Pattern): string {
    return [
      pattern.subject.value,
      pattern.predicate.value,
      pattern.object.value,
      pattern.graph?.value ?? '',
    ].join('|');
  }
}
