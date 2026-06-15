import {
  type IActionRdfJoin,
  type IActorRdfJoinOutputInner,
  type IActorRdfJoinArgs,
  type MediatorRdfJoin,
  type IActorRdfJoinTestSideData,
  ActorRdfJoin,
} from '@comunica/bus-rdf-join';
import { KeysCaching, KeysInitQuery, KeysQueryOperation } from '@comunica/context-entries';
import { TestResult, IActorTest, passTestVoid, failTest, passTestWithSideData } from '@comunica/core';
import { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import { KEY_IS_SUB_STAR } from '@comunica/actor-rdf-join-inner-multi-cached-csets-cps';
import { ComunicaDataFactory, IJoinEntry } from '@comunica/types';
import { Algebra, AlgebraFactory } from '@comunica/utils-algebra';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import { CsetBasedStarJoinEstimator } from '@comunica/actor-rdf-join-inner-multi-cached-csets-cps';
import { MediatorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import { CacheKey, ICacheKey, IViewKey, ViewKey } from '@comunica/cache-manager-entries';
import { IReachableDataSummary } from '@comunica/actor-optimize-query-operation-set-cache-cset-get-view';
import { dpSub } from './LeftDeepStarSampler';
import { getSafeBindings } from '@comunica/utils-query-operation';

/**
 * A comunica Inner Multi Star Cset Dp RDF Join Actor.
 */
export class ActorRdfJoinInnerMultiStarCsetDp extends ActorRdfJoin {
  protected readonly csetBasedStarJoinEstimator = new CsetBasedStarJoinEstimator();
  public readonly mediatorJoinEntriesSort: MediatorRdfJoinEntriesSort;
  public readonly mediatorJoin: MediatorRdfJoin;
  
  protected readonly cacheEntryKey: ICacheKey<unknown, unknown, unknown>
  // A view over the cache that allows cache queries using quads
  protected readonly cacheGlobalStatsViewKey: 
    IViewKey<unknown, { [key: string]: any }, IReachableDataSummary>;
  
  public constructor(args: IActorRdfJoinMultiStarCsetDpArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'multi-star-dp',
      limitEntries: 3,
      limitEntriesMin: true,
      canHandleUndefs: true,
      isLeaf: false,
    });
    this.mediatorJoinEntriesSort = args.mediatorJoinEntriesSort;
    this.mediatorJoin = args.mediatorJoin;

    this.cacheEntryKey = new CacheKey(args.cacheEntryName);
    this.cacheGlobalStatsViewKey = new ViewKey(args.cacheGlobalStatsViewName);
  }

  public override async test(
    action: IActionRdfJoin,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinTestSideData>> {
    const context = action.context;

    if (!context.has(KEY_IS_SUB_STAR)){
      return failTest(`${this.name} can only optimize star-shaped sub-queries`);
    }

    return super.test(action);
  }

  protected async getOutput(
    action: IActionRdfJoin,
    sideData: IActorRdfJoinTestSideData,
  ): Promise<IActorRdfJoinOutputInner> {
    // TODO: Should get global summary then bind global summary + type of star
    // to the star cardinality estimation method.
    // Then run DP.
    // From output order set each cardinality and do binary join
    // return output.

    const context = action.context;
    const entries: IJoinEntry[] = action.entries;

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

    if (!globalDataSummary){
      throw new Error(`${this.name}: Missing data summary`);
    }

    // Always set to subject star for now. When it works also implement object star
    const estimate = (starEntries: IJoinEntry[]) => {
      const starPatterns = starEntries.map(entry => <Algebra.Pattern> entry.operation);
      return this.csetBasedStarJoinEstimator.estimateStarCardinality(starPatterns, "subject", globalDataSummary);
    }
    const order = await dpSub(entries, estimate);
    console.log(order);
    const firstLeftJoin = [ order.plan[0], order.plan[1]].map(idx => entries[idx]);
    
    // Get first entry which will be the left-side of our left-deep plan
    const firstJoinResult: IJoinEntry = {
      output: getSafeBindings(await this.mediatorJoin.mediate(
        { 
          type: action.type, 
          entries: firstLeftJoin,
          context: action.context 
        }
      )),
      operation: algebraFactory
        .createJoin(firstLeftJoin.map(entry => entry.operation), false),
    };
    // First entry of step cardinality is left triple pattern cardinality
    // so start from index = 1
    this.updateCardinalityMetadata(firstJoinResult, order.stepCardinalities[1]);

    let leftDeepResult = firstJoinResult;
    // Join rest of entries in order
    for (let i = 2; i < order.plan.length; i++){
      const nextEntry = order.plan[i];
      leftDeepResult = {
        output: getSafeBindings(await this.mediatorJoin.mediate(
          { 
            type: action.type, 
            entries: [leftDeepResult, entries[nextEntry]],
            context: action.context 
          }
        )),
        operation: algebraFactory
          .createJoin([leftDeepResult.operation, entries[nextEntry].operation], false),
      };
      this.updateCardinalityMetadata(leftDeepResult, order.stepCardinalities[i]);
    }

    return {
      result: leftDeepResult.output,
    };    
  }

  protected async updateCardinalityMetadata(
    entry: IJoinEntry, cardinalityValue: number
  ){
    const existingMetadata = await entry.output.metadata();
    entry.output.metadata = async () => {
      existingMetadata.cardinality.value = cardinalityValue;
      existingMetadata.cardinality.type = 'estimate';
      return existingMetadata;
    }
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



export interface IActorRdfJoinMultiStarCsetDpArgs extends IActorRdfJoinArgs<IActorRdfJoinTestSideData> {
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
}