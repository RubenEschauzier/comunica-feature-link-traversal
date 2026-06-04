import type {
  IActionRdfJoin,
  IActorRdfJoinOutputInner,
  IActorRdfJoinArgs,
  MediatorRdfJoin,
  IActorRdfJoinTestSideData,
} from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { MediatorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import { CacheKey, ICacheKey, IViewKey, ViewKey } from '@comunica/cache-manager-entries';
import { KeysCaching, KeysInitQuery } from '@comunica/context-entries';
import type { TestResult } from '@comunica/core';
import { ActionContextKey, failTest, passTestWithSideData } from '@comunica/core';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type {
  IJoinEntry,
  IActionContext,
  IJoinEntryWithMetadata,
  ComunicaDataFactory,
} from '@comunica/types';
import { Algebra, AlgebraFactory } from '@comunica/utils-algebra';
import { getSafeBindings } from '@comunica/utils-query-operation';
import { IReachableDataSummary } from '../../actor-optimize-query-operation-set-cache-cset-get-view/lib';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';

/**
 * A Multi Smallest RDF Join Actor.
 * It accepts 3 or more streams, joins the smallest two, and joins the result with the remaining streams.
 */
export class ActorRdfJoinMultiCachedCsetsCps extends ActorRdfJoin<IActorRdfJoinTestSideData> {
  public readonly mediatorJoinEntriesSort: MediatorRdfJoinEntriesSort;
  public readonly mediatorJoin: MediatorRdfJoin;
  public readonly minCacheEntries: number;
  
  protected readonly cacheEntryKey: ICacheKey<unknown, unknown, unknown>
  // A view over the cache that allows cache queries using quads
  protected readonly cacheGlobalStatsViewKey: IViewKey<unknown, { [key: string]: any }, IReachableDataSummary>
  
  public constructor(args: IActorRdfJoinMultiCachedCsetsCpsArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'multi-smallest',
      limitEntries: 3,
      limitEntriesMin: true,
      canHandleUndefs: true,
      isLeaf: false,
    });
    this.mediatorJoinEntriesSort = args.mediatorJoinEntriesSort;
    this.mediatorJoin = args.mediatorJoin;
    this.minCacheEntries = args.minCacheEntries;

    this.cacheEntryKey = new CacheKey(args.cacheEntryName);
    this.cacheGlobalStatsViewKey = new ViewKey(args.cacheGlobalStatsViewName);
  }

  public override async  test(
    action: IActionRdfJoin,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinTestSideData>> {
    const context = action.context;
    if (context.has(KEY_CONTEXT_WRAPPED)){
      return failTest(`${this.name} can only wrap the the join execution once`);
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
    return super.test(action);
  }

  /**
   * Finds join indexes of lowest cardinality result sets, with priority on result sets that have common variables
   * @param entries A sorted array of entries, sorted on cardinality
   */
  public getJoinIndexes(entries: IJoinEntryWithMetadata[]) {
    // Iterate over all combinations of join indexes, return the first combination that does not lead to a cartesian product
    for (let i = 0; i < entries.length; i++) {
      for (let j = i + 1; j < entries.length; j++) {
        if (this.hasCommonVariables(entries[i], entries[j])) {
          return [ i, j ];
        }
      }
    }
    // If all result sets are disjoint we just want the sets with lowest cardinality
    return [ 0, 1 ];
  }

  public hasCommonVariables(entry1: IJoinEntryWithMetadata, entry2: IJoinEntryWithMetadata): boolean {
    const variableNames1 = entry1.metadata.variables.map(x => x.variable.value);
    const variableNames2 = new Set(entry2.metadata.variables.map(x => x.variable.value));
    return variableNames1.some(v => variableNames2.has(v));
  }

  /**
   * Order the given join entries using the join-entries-sort bus.
   * @param {IJoinEntryWithMetadata[]} entries An array of join entries.
   * @param context The action context.
   * @return {IJoinEntryWithMetadata[]} The sorted join entries.
   */
  public async sortJoinEntries(
    entries: IJoinEntryWithMetadata[],
    context: IActionContext,
  ): Promise<IJoinEntryWithMetadata[]> {
    return (await this.mediatorJoinEntriesSort.mediate({ entries, context })).entries;
  }

  protected async getOutput(
    action: IActionRdfJoin,
    sideData: IActorRdfJoinTestSideData,
  ): Promise<IActorRdfJoinOutputInner> {
    const context = action.context;

    const dataFactory: ComunicaDataFactory = action.context.getSafe(KeysInitQuery.dataFactory);
    const algebraFactory = new AlgebraFactory(dataFactory);

    const seeds = context.getSafe(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager).seeds;
    const query = context.getSafe(KeysInitQuery.query);

    const persistentCacheManager = context.getSafe(KeysCaching.cacheManager);

    const globalDataSummary = await persistentCacheManager.getFromCache(
      this.cacheEntryKey,
      this.cacheGlobalStatsViewKey,
      { seeds, query },
    );

    // Determine the two smallest streams by sorting (e.g. via cardinality)
    const entries: IJoinEntry[] = action.entries;

    return {
      result: await this.mediatorJoin.mediate({
        type: action.type,
        entries,
        context: context.set(KEY_CONTEXT_WRAPPED, true),
      }),
    };
  }

  protected async getJoinCoefficients(
    action: IActionRdfJoin,
    sideData: IActorRdfJoinTestSideData,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinTestSideData>> {
    return passTestWithSideData({
      iterations: 0,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: 0,
    }, { ...sideData });
  }
}

export interface IActorRdfJoinMultiCachedCsetsCpsArgs extends IActorRdfJoinArgs<IActorRdfJoinTestSideData> {
  /**|
   * Name of the key for obtaining the cache used in this join actor
   */
  cacheEntryName: string;
  /**
   * Name of the key of for the view producing the global csets and cps
   */
  cacheGlobalStatsViewName: string;
  /**
   * The join entries sort mediator
   */
  mediatorJoinEntriesSort: MediatorRdfJoinEntriesSort;
  /**
   * A mediator for joining Bindings streams
   */
  mediatorJoin: MediatorRdfJoin;
  /**
   * Minimal number of documents cached before this actor can start making join orders
   */
  minCacheEntries: number
}


export const KEY_CONTEXT_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-join-inner-multi-cached-csets-cps:wrapped',
);
