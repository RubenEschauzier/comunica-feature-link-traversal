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
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { getSafeBindings } from '@comunica/utils-query-operation';
import { IReachableDataSummary } from '../../actor-optimize-query-operation-set-cache-cset-get-view/lib';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import type * as RDF from '@rdfjs/types';
import { Pattern } from '@comunica/utils-algebra/lib/Algebra';
import * as RdfString from 'rdf-string';
import { ICharacteristicSet, PersistentCacheCset } from '@comunica/caches-link-traversal';

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

    // TODO: 
    // Finish implementation that divides query into meta nodes to decrease size query.


    // Determine how to use csets to improve Comunica estimator for star-shaped queries.
    // Also determine how to use DP-based multi-join in framework for joins
    // Implement selectivity estimator based on csets and cps

    // Only run this actor if we have a data summary, otherwise we just
    // let Comunica handle it
    if (globalDataSummary){
      const entriesMetadata = await ActorRdfJoin.getEntriesWithMetadatas(entries);
      // Collapse the subject stars into meta nodes if optimizer says so
      // In this function we set the cardinality of this node properly to ensure
      // the Comunica optimizer can then handle these nodes properly in optimization
      this.getMetaNodeSubjectStar(entriesMetadata, globalDataSummary);
    }


    // Then collapse these into meta nodes and determine other join orders


    // Return the complete joined result
    return {
      result: await this.mediatorJoin.mediate({
        type: action.type,
        entries,
        context: context.set(KEY_CONTEXT_WRAPPED, true),
      }),
    };
  }

  protected getMetaNodeSubjectStar(
    entriesMetadata: IJoinEntryWithMetadata[],
    globalDataSummary: IReachableDataSummary
  ) {
    // Find smallest cardinality that is not zero and estimated and compare ratio to the
    const cardinalitiesQuery = entriesMetadata.map((e) => e.metadata.cardinality);
    const subjectStars = this.getSubjectStars(entriesMetadata);
    for (const subjectStar of subjectStars) {
      const starPatterns = subjectStar.entries.map(e => <Algebra.Pattern>e.operation);
      // Compute and assign cardinality directly
      subjectStar.cardinality = this.estimateStarCardinality(
        starPatterns, 
        "subject", 
        globalDataSummary
      );
      
      // Collapse the star into one node if not too far off from smallest cardinality in
      // query

    }
  }

  protected estimateStarCardinality(
    starPatterns: Algebra.Pattern[],
    starType: "subject" | "object",
    globalDataSummary: IReachableDataSummary
  ): number {
    const isBound = (term: RDF.Term): boolean => 
      term.termType !== 'Variable' && term.termType !== 'BlankNode';

    const centerNode = starType === 'subject' ? starPatterns[0].subject : starPatterns[0].object;
    const isCenterBound = isBound(centerNode);

    let matchingCsetKeys: Set<string> | undefined;
    let missingBoundValue = false;

    if (isCenterBound) {
      // Try to find cset belonging to bound center node
      const hash = PersistentCacheCset.hashTerm(centerNode);
      
      let boundCenterCset: ICharacteristicSet | undefined = undefined;
      if (starType === 'subject'){
        boundCenterCset = globalDataSummary.subjectToCset.get(hash);
      }
      else {
        throw new Error("Object stars not yet supported");
      }

      if (!boundCenterCset){
        missingBoundValue = true;
      }
      else {
        matchingCsetKeys = new Set([ boundCenterCset.predKey ]); 
      }
    } 
    if (missingBoundValue || !isCenterBound) {
      // For unbound center values or for bound values with missing csets we
      // execute normal matching. When a bound value is missing we aren't sure
      // the bound value doesn't exist as it can be missing from cache
      // so we estimate using default approach and divide by number of unique
      // values possible in that position
      matchingCsetKeys = this.getSupersetKeys(starPatterns, globalDataSummary);
      if (matchingCsetKeys.size === 0) {
        return 0;
      } 
    }
    return this.calculateCardinalityClamped(
      matchingCsetKeys!, 
      globalDataSummary, 
      starPatterns, 
      starType, 
      isBound,
      isCenterBound,
      missingBoundValue,
    );
  }

  /**
   * Get keys of all cset supersets of the current patterns in the star
   */
  protected getSupersetKeys(
    starPatterns: Algebra.Pattern[],
    globalDataSummary: IReachableDataSummary
  ): Set<string> {
    const isBound = (term: RDF.Term): boolean => term.termType !== 'Variable';
    const csetKeySets: Set<string>[] = [];

    for (const pattern of starPatterns) {
      if (isBound(pattern.predicate)) {
        const keysForPred = globalDataSummary.predToCset.get(
          RdfString.termToString(pattern.predicate)
        );
        if (!keysForPred) {
          return new Set();
        }
        csetKeySets.push(keysForPred);
      }
    }

    if (csetKeySets.length === 0) {
      return new Set();
    }
    return this.intersectMultipleSets(csetKeySets);
  }  


  protected calculateCardinalityClamped(    
    csetsSuperSet: Set<string>,
    globalDataSummary: IReachableDataSummary,
    starPatterns: Algebra.Pattern[],
    starType: "subject" | "object",
    isBound: (term: RDF.Term) => boolean,
    isCenterBound: boolean,
    missingBoundValue: boolean,
  ){
    const estimation = this.calculateCardinality(
      csetsSuperSet,
      globalDataSummary,
      starPatterns,
      starType,
      isBound,
      isCenterBound,
      missingBoundValue,
    )
    return Math.max(1, Math.ceil(estimation));
  }

  protected calculateCardinality(
    csetsSuperSet: Set<string>,
    globalDataSummary: IReachableDataSummary,
    starPatterns: Algebra.Pattern[],
    starType: "subject" | "object",
    isBound: (term: RDF.Term) => boolean,
    isCenterBound: boolean,
    missingBoundValue: boolean,
  ): number {
    let totalUnboundCardinality = 0;
    let totalMatchingSubjects = 0;

    for (const csetKey of csetsSuperSet) {
      const cset = globalDataSummary.csets.get(csetKey)!;
      
      let m = 1;
      let o = 1;

      for (const pattern of starPatterns) {
        const isPredBound = isBound(pattern.predicate);
        const isPeripheralBound = starType === 'subject' 
          ? isBound(pattern.object) 
          : isBound(pattern.subject);

        // Handle unbound predicates (e.g., ?s ?p ?o)
        if (!isPredBound) {
          let totalEdges = 0;
          for (const count of cset.predicateCounts.values()) {
            totalEdges += count;
          }

          // Multiply by average total edges per subject in this CSet
          m *= (totalEdges / cset.subjCount);
          
          if (isPeripheralBound) {
            o = Math.min(o, 1 / cset.subjCount);
          }
          continue; 
        }

        const pred = pattern.predicate.value;
        const predCount = cset.predicateCounts.get(pred);

        // If the exact CSet lacks this predicate, cardinality is 0
        if (predCount === undefined) {
          if (!isCenterBound){
            throw new Error("Center of star is not bound but we found a predicate in query not in cset");
          }
          m = 0;  
          break;
        }

        // Handle bound predicates
        if (isPeripheralBound) {
          // Correct conditional selectivity using predicate count, not subject count
          const conditionalSelectivity = 1 / cset.predicateCounts.get(pred)!; 
          o = Math.min(o, conditionalSelectivity);
        } else {
          // Calculate average predicate occurrences
          m *= (cset.predicateCounts.get(pred)! / cset.subjCount);
        }      
      }

      // If the center is bound, the distinct entity count for this CSet drops to 1.
      // Otherwise, use the full subject count of the CSet.
      if (isCenterBound && !missingBoundValue){
        totalUnboundCardinality += 1 * m * o;
      } else {
        totalUnboundCardinality += cset.subjCount * m * o;
        totalMatchingSubjects += cset.subjCount;
      }
    }

    let finalEstimation: number;
    // We make our estimation based on average cardinality of a subject with these patterns
    if (isCenterBound && missingBoundValue){
      if (totalMatchingSubjects === 0){
        finalEstimation = 0;
      }
      else { 
        finalEstimation = totalUnboundCardinality / totalMatchingSubjects;
      }
    }      
    else {
      finalEstimation = totalUnboundCardinality;
    }
    return finalEstimation
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


  protected intersectSets<T>(setA: Set<T>, setB: Set<T>): Set<T> {
    if (setA.size > setB.size) {
      return this.intersectSets(setB, setA);
    }

    const intersection = new Set<T>();
    for (const item of setA) {
      if (setB.has(item)) {
        intersection.add(item);
      }
    }

    return intersection;
  }

  protected intersectMultipleSets<T>(sets: Set<T>[]): Set<T> {
    if (sets.length === 0) {
      return new Set<T>();
    }

    // Sort sets by size ascending
    sets.sort((a, b) => a.size - b.size);

    let result = sets[0];

    for (let i = 1; i < sets.length; i++) {
      result = this.intersectSets(result, sets[i]);
      
      // Early exit if the intersection is empty
      if (result.size === 0) {
        break;
      }
    }

    return result;
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
