import { IReachableDataSummary } from '@comunica/actor-optimize-query-operation-set-cache-cset-get-view';
import { ActorRdfJoinSelectivity, IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput, IActorRdfJoinSelectivityArgs } from '@comunica/bus-rdf-join-selectivity';
import { CacheKey, ICacheKey, IViewKey, ViewKey } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries';
import { TestResult, IActorTest, passTestVoid, passTest, failTest } from '@comunica/core';
import { IMediatorTypeAccuracy } from '@comunica/mediatortype-accuracy';

/**
 * A comunica Cset Cp RDF Join Selectivity Actor.
 */
export class ActorRdfJoinSelectivityCsetCp extends ActorRdfJoinSelectivity {
  public readonly minCacheEntries: number;
  
  protected readonly cacheEntryKey: ICacheKey<unknown, unknown, unknown>
  // A view over the cache that allows cache queries using quads
  protected readonly cacheGlobalStatsViewKey: 
    IViewKey<unknown, { [key: string]: any }, IReachableDataSummary>;
  
  public constructor(args: IActorRdfJoinSelectivityCsetCpArgs) {
    super(args);

    this.minCacheEntries = args.minCacheEntries;
    this.cacheEntryKey = new CacheKey(args.cacheEntryName);
    this.cacheGlobalStatsViewKey = new ViewKey(args.cacheGlobalStatsViewName);
  }

  public async test(action: IActionRdfJoinSelectivity): Promise<TestResult<IMediatorTypeAccuracy>> {
    const context = action.context;
    const persistentCacheManager = context.get(KeysCaching.cacheManager);
    if (
      !persistentCacheManager ||
      !persistentCacheManager.hasCache(this.cacheEntryKey) || 
      !persistentCacheManager.hasView(this.cacheGlobalStatsViewKey)
    ){
      return failTest(`${this.name} requires the cache manager to contain caches for csets and cps`);
    }

    const sizeCache = await persistentCacheManager.getRegisteredCache(this.cacheEntryKey!)!.cache.size();
    if (sizeCache < this.minCacheEntries){
      return failTest(`${this.name} requires atleast ${this.minCacheEntries} cached documents to run`);
    }
    return passTest({ accuracy: 0.8});
  }

  public async run(action: IActionRdfJoinSelectivity): Promise<IActorRdfJoinSelectivityOutput> {
    // Uses the CP algorithm to determine join selectivity between two entries.
    return true; // TODO implement
  }
}


export interface IActorRdfJoinSelectivityCsetCpArgs extends IActorRdfJoinSelectivityArgs {
  /**|
   * Name of the key for obtaining the cache used in this join actor
   */
  cacheEntryName: string;
  /**
   * Name of the key of for the view producing the global csets and cps
   */
  cacheGlobalStatsViewName: string;
  /**
   * Minimal number of documents cached before this actor can start making join orders
   */
  minCacheEntries: number;
}
