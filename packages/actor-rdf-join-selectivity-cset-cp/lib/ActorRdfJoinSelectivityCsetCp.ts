import { IReachableDataSummary } from '@comunica/actor-optimize-query-operation-set-cache-cset-get-view';
import { ActorRdfJoinSelectivity, IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput, IActorRdfJoinSelectivityArgs } from '@comunica/bus-rdf-join-selectivity';
import { CacheKey, ICacheKey, IViewKey, ViewKey } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries';
import { TestResult, IActorTest, passTestVoid, passTest, failTest } from '@comunica/core';
import { IMediatorTypeAccuracy } from '@comunica/mediatortype-accuracy';
import { Algebra, algebraUtils } from '@comunica/utils-algebra';

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
    if (action.entries.length > 0){
      return failTest(`${this.name} can only estimate selectivity for two entries`)
    }
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
    console.log(action.entries.map(entry => entry.operation));
    // TODO: How to deal with property paths?
    const operations = action.entries.map(entry => entry.operation);
    const operationsPatternsOrPaths = this.getPatternsOrPaths(operations);
    const predicates = operationsPatternsOrPaths.map((patternOrPathOperation) => 
      patternOrPathOperation.map(patternOrPath => 
        patternOrPath.predicate.termType !== 'NamedNode'
      )
    )
    // Uses the CP algorithm to determine join selectivity between two entries.
    return {
      selectivity: 1
    }; // TODO implement
  }

  public getPatternsOrPaths(operations: Algebra.BaseOperation[]) {
    const patterns: (Algebra.Pattern | Algebra.Path)[][] = [];
    for (const operation of operations) {
      const operationPatterns: (Algebra.Pattern | Algebra.Path)[] = [];
      algebraUtils.visitOperation(operation, {
        [Algebra.Types.PATTERN]: { preVisitor: (pattern) => {
          operationPatterns.push(pattern);
          return { continue: false };
        } },
        [Algebra.Types.PATH]: { preVisitor: (path) => {
          operationPatterns.push(path);
          return { continue: false };
        } },
      });
      patterns.push(operationPatterns);
    }
    return patterns;
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
