import { ActorOptimizeQueryOperation, IActionOptimizeQueryOperation, IActorOptimizeQueryOperationOutput, IActorOptimizeQueryOperationArgs } from '@comunica/bus-optimize-query-operation';
import { CacheEntrySourceState, CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysInitQuery } from '@comunica/context-entries';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, passTestVoid, failTest, ActionContext } from '@comunica/core';
import { QuerySourceUnidentified } from '@comunica/types';

/**
 * A comunica Initialize Source Link Traversal Cache Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationInitializeSourceLinkTraversalCache extends ActorOptimizeQueryOperation {
  public constructor(args: IActorOptimizeQueryOperationArgs) {
    super(args);
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    const cacheManager = action.context.get(KeysCaching.cacheManager);
    if (!cacheManager){
      failTest(`${this.name} cannot add a cache-based query source without a cache manager object in context`);
    }
    // TODO: Here must be a way to test if this manager has a cache registered that can do what this actor needs it to do.
    // for example this cache should have a function that allows me to queryBindings
    // Possibility: make view keys and cache keys components.js objects and configure them in this actor, seems heavy though
    if (!cacheManager!.getRegisteredCache(CacheEntrySourceState.cacheSourceStateQuerySource)){
      failTest(`${this.name} requires cache: ${CacheEntrySourceState.cacheSourceStateQuerySource.id}`);
    }
    if (!cacheManager!.getRegisteredView(CacheSourceStateViews.cacheQueryView)){
      failTest(`${this.name} requires view: ${CacheSourceStateViews.cacheQueryView.id}`);
    }
    return passTestVoid();
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    let context = action.context;
    let querySources: QuerySourceUnidentified[] | undefined = context.get(KeysInitQuery.querySourcesUnidentified);
    if (!querySources){
      querySources = [];
    }

    // Add query source based on a cache exposing these views
    querySources.push({
      type: 'cache',
      value: "temp (not sure what to do with this)",
      cacheKey: CacheEntrySourceState.cacheSourceStateQuerySource,
      getSource: CacheSourceStateViews.cacheQueryView,
      count: CacheSourceStateViews.cacheCountView,
      context: new ActionContext(
        {[KeysCaching.cacheManager.name]: action.context.getSafe(KeysCaching.cacheManager)}
      ),
    });
    context = context.set(KeysInitQuery.querySourcesUnidentified, querySources);
    
    return {context, operation: action.operation};
  }
}
