import { ActorContextPreprocess, IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries';
import { TestResult, IActorTest, passTestVoid, ActionContext } from '@comunica/core';
import { ICacheView, IPersistentCache, ISourceStateBloomFilter } from '@comunica/types';
import { Algebra, isKnownOperation } from '@comunica/utils-algebra';

/**
 * A comunica Set Cache Count View Context Preprocess Actor.
 */
export class ActorContextPreprocessSetCacheCountBloomFilterView extends ActorContextPreprocess {
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
      CacheSourceStateViews.cacheCountBloomFilterView,
      new CacheCountBloomFilterView(),
    );
    return { context };
  }
}

export class CacheCountBloomFilterView
implements ICacheView<ISourceStateBloomFilter, { operation: Algebra.Operation, documents: string[] }, number> {
  public async construct(
    cache: IPersistentCache<ISourceStateBloomFilter>,
    context: { operation: Algebra.Operation, documents: string[] }
  ): Promise<number | undefined> {
    if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
      let totalCount = 0;
      const cacheEntryStream = cache.entries();

      for await (const [key, source] of cacheEntryStream) {
        if (source.source.countQuads && this.shouldQuerySource(source, context.operation)) {
          const quadCount = await source.source.countQuads(context.operation, new ActionContext());
          totalCount += quadCount;
        }
      }
      return totalCount;
    }
    throw new Error("Count view only accepts quad patterns");
  }

  protected shouldQuerySource(source: ISourceStateBloomFilter, operation: Algebra.Pattern): boolean {
    if (!source.bloomFilter) {
      return true;
    }
    if (operation.subject.termType !== 'Variable' && !source.bloomFilter.has(operation.subject.value)) {
      return false;
    }
    if (operation.object.termType !== 'Variable' && !source.bloomFilter.has(operation.object.value)) {
      return false;
    }
    return true;
  }
}