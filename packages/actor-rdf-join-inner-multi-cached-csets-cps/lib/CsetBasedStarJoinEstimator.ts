import { IReachableDataSummary } from "@comunica/actor-optimize-query-operation-set-cache-cset-get-view";
import { ICharacteristicSet, PersistentCacheCset } from "@comunica/caches-link-traversal";
import { Algebra } from "@comunica/utils-algebra";
import type * as RDF from '@rdfjs/types';
import * as RdfString from 'rdf-string';

export class CsetBasedStarJoinEstimator{
    public estimateStarCardinality(
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