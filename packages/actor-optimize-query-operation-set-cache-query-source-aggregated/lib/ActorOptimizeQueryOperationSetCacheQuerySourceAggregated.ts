import { ActorOptimizeQueryOperation, IActionOptimizeQueryOperation, IActorOptimizeQueryOperationOutput, IActorOptimizeQueryOperationArgs } from '@comunica/bus-optimize-query-operation';
import { KeysQuerySourceIdentify } from '@comunica/context-entries';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, passTestVoid } from '@comunica/core';

/**
 * A comunica Set Cache Query Source Aggregated Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheQuerySourceAggregated extends ActorOptimizeQueryOperation {
  public constructor(args: IActorOptimizeQueryOperationSetCacheQuerySourceAggregatedArgs) {
    super(args);
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    return passTestVoid(); // TODO implement
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    const context = action.context;
    if (!action.context.get(KeysQuerySourceIdentify.traverse)){
      return { context, operation: action.operation };
    }

    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceStateQuerySource,
      this.cacheQuerySourceState,
      new SetSourceStateCache(),
    );

    cacheManager.registerCacheView(
      CacheSourceStateViews.cacheQueryView,
      new GetSourceStateCacheView(),
    );
    
    return { context, operation: action.operation };
  }
}

export interface IActorOptimizeQueryOperationSetCacheQuerySourceAggregatedArgs extends IActionOptimizeQueryOperation{
    /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124000}
   */
  cacheSizeNumTriples: number;
}