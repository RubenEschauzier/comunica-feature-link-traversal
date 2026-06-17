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
import { KeysCaching, KeysInitQuery, KeysQueryOperation } from '@comunica/context-entries';
import type { TestResult } from '@comunica/core';
import { ActionContextKey, failTest, passTestWithSideData } from '@comunica/core';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type {
  IJoinEntry,
  IActionContext,
  IJoinEntryWithMetadata,
  ComunicaDataFactory,
} from '@comunica/types';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { getSafeBindings } from '@comunica/utils-query-operation';
import { IReachableDataSummary } from '../../actor-optimize-query-operation-set-cache-cset-get-view/lib';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import type * as RDF from '@rdfjs/types';
import { Pattern } from '@comunica/utils-algebra/lib/Algebra';
import * as RdfString from 'rdf-string';
import { ICharacteristicSet, PersistentCacheCset } from '@comunica/caches-link-traversal';
import { CsetBasedStarJoinEstimator } from './CsetBasedStarJoinEstimator';

/**
 * A Multi Smallest RDF Join Actor.
 * It accepts 3 or more streams, joins the smallest two, and joins the result with the remaining streams.
 */
export class ActorRdfJoinMultiCachedCsetsCps extends ActorRdfJoin<IActorRdfJoinTestSideData> {
  public readonly mediatorJoinEntriesSort: MediatorRdfJoinEntriesSort;
  public readonly mediatorJoin: MediatorRdfJoin;
  public readonly minCacheEntries: number;
  public readonly maxRatioCardinality: number;
  
  protected readonly cacheEntryKey: ICacheKey<unknown, unknown, unknown>
  // A view over the cache that allows cache queries using quads
  protected readonly cacheGlobalStatsViewKey: 
    IViewKey<unknown, { [key: string]: any }, IReachableDataSummary>;
  
  public constructor(args: IActorRdfJoinMultiCachedCsetsCpsArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'multi-cached-csets-cps',
      limitEntries: 3,
      limitEntriesMin: true,
      canHandleUndefs: true,
      isLeaf: false,
    });
    this.mediatorJoinEntriesSort = args.mediatorJoinEntriesSort;
    this.mediatorJoin = args.mediatorJoin;
    this.minCacheEntries = args.minCacheEntries;
    this.maxRatioCardinality = args.maxRatio;

    this.cacheEntryKey = new CacheKey(args.cacheEntryName);
    this.cacheGlobalStatsViewKey = new ViewKey(args.cacheGlobalStatsViewName);
  }

  public override async  test(
    action: IActionRdfJoin,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinTestSideData>> {
    const context = action.context;

    if(context.get(KeysQueryOperation.joinBindings) !== undefined){
      return failTest(`${this.name} can only wrap top-level join executions`);
    }

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

  protected getSubjectStars(entries: IJoinEntryWithMetadata[]) {
    const starEntries: {subject: RDF.Quad_Subject, entries: IJoinEntryWithMetadata[], cardinality: number }[] = [];
    for (const entry of entries){
      const operation = entry.operation;
      // This guard should always evaluate to true as this actor is a top-level join
      if (isKnownOperation(operation, Algebra.Types.PATTERN)) {
        const subject = <RDF.Quad_Subject> operation.subject;

        // Find existing entry and stop iterating
        const existingStar = starEntries.find((star) => star.subject.equals(subject));

        if (existingStar) {
          existingStar.entries.push(entry);
        } else {
          starEntries.push({ subject, entries: [entry], cardinality: 0 });
        }
      }
    }
    return starEntries.filter((starEntry) => starEntry.entries.length > 1);
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

    // Determine how to use csets to improve Comunica estimator for star-shaped queries.
    // Also determine how to use DP-based multi-join in framework for joins
    // Implement selectivity estimator based on csets and cps

    // Only run this actor if we have a data summary, otherwise we just
    // let Comunica handle it
    if (globalDataSummary){
      const entriesMetadata = await ActorRdfJoin.getEntriesWithMetadatas(entries);

      // Collapse the subject stars into meta nodes. This mutates the action.entries
      // to include the meta nodes and removes aggregated triple patterns
      await this.contractSubjectStarToMetaNode(
        action, 
        entriesMetadata, 
        globalDataSummary,
        algebraFactory
      );
    }
    console.log(`Done collapsing: ${entries.length} left`);
    // Return the complete joined result
    return {
      result: await this.mediatorJoin.mediate({
        type: action.type,
        entries: action.entries,
        context: context.set(KEY_CONTEXT_WRAPPED, true),
      }),
    };
  }

  protected async contractSubjectStarToMetaNode(
    action: IActionRdfJoin,
    entriesMetadata: IJoinEntryWithMetadata[],
    globalDataSummary: IReachableDataSummary,
    algebraFactory: AlgebraFactory,
  ) {
    const starEstimator = new CsetBasedStarJoinEstimator();
    // Find smallest cardinality that is non-zero and estimated and compare ratio to the
    // subject stars
    const cardinalitiesQuery = entriesMetadata.map(
      (e) => {
        const c = e.metadata.cardinality;
        return c.type === 'estimate' ? Math.max(1, c.value): c.value;
      }        
    );
    const minCard = Math.min(...cardinalitiesQuery);

    const subjectStars = this.getSubjectStars(entriesMetadata);

    for (const subjectStar of subjectStars) {
      const starPatterns = subjectStar.entries.map(e => <Algebra.Pattern>e.operation);
      // Estimate star cardinality using cached characteristic sets
      subjectStar.cardinality = starEstimator.estimateStarCardinality(
        starPatterns, 
        "subject", 
        globalDataSummary
      );

      // Collapse the star into one node if not too far off from smallest cardinality in
      // query
      if (subjectStar.cardinality < minCard * this.maxRatioCardinality){
        console.log(`Collapsing star of size: ${subjectStar.entries}`);
        // Call join algorithm to obtain streaming output of this join.
        const subStarOutput = await this.mediatorJoin.mediate({
          type: action.type,
          entries: subjectStar.entries,
          context: action.context
            .set(KEY_CONTEXT_WRAPPED, true)
            .set(KEY_IS_SUB_STAR, true),
        });
        const originalMetadataFn = subStarOutput.metadata;

        // Override the metadata output to set the cardinality value
        subStarOutput.metadata = async () => {
          const metadata = await originalMetadataFn();
          metadata.cardinality.type = 'estimate';
          metadata.cardinality.value = subjectStar.cardinality;
          return metadata;
        };

        // Remove the contracted nodes from the original action
        const operationsToRemove = new Set(subjectStar.entries.map(e => e.operation));
        for (let i = action.entries.length - 1; i >= 0; i--) {
          if (operationsToRemove.has(action.entries[i].operation)) {
            action.entries.splice(i, 1);
          }
        }

        const operations = subjectStar.entries.map((entry) => entry.operation);
        // console.log(subStarOutput);
        const joinOperation = algebraFactory.createJoin(operations, true);

        action.entries.push({
          output: subStarOutput,
          operation: joinOperation
        });
      }
    }
    return action;
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
  /**
   * Maximum ratio between smallest cardinality triple pattern and the sub-stars
   * we want to collapse. So for a ratio of 10 the star cardinality can be maximally
   * 10 times larger than the smallest cardinality entry (also within star?).
   * TODO determine default
   * @default {5}
   */
  maxRatio: number
}


export const KEY_CONTEXT_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-join-inner-multi-cached-csets-cps:wrapped',
);

export const KEY_IS_SUB_STAR = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-join-inner-multi-cached-csets-cps:isSubStar'
)