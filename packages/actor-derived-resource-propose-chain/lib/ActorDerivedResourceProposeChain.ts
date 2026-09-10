import { IDerivedResource } from '@comunica/actor-extract-links-solid-derived-resources';
import { ActorDerivedResourcePropose, IActionDerivedResourcePropose, IActorDerivedResourceProposeOutput, IActorDerivedResourceProposeArgs } from '@comunica/bus-derived-resource-propose';
import { TestResult, IActorTest, passTestVoid } from '@comunica/core';
import { FragmentSelectorShape } from '@comunica/types';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';
import { canAnswerBgp } from '@comunica/utils-query-operation';
import type * as RDF from '@rdfjs/types';

/**
 * A comunica Chain Derived Resource Propose Actor.
 */
export class ActorDerivedResourceProposeChain extends ActorDerivedResourcePropose {
  protected readonly algebraFactory = new AlgebraFactory();
  protected readonly maxChainLength: number;

  public constructor(args: IActorDerivedResourceProposeChainArgs) {
    super(args);
    this.maxChainLength = args.maxChainLength;

  }

  public async test(action: IActionDerivedResourcePropose): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourcePropose): Promise<IActorDerivedResourceProposeOutput> {
    const bgps: Algebra.Bgp[] = [];
    algebraUtils.visitOperation(action.operation, {
      [Algebra.Types.BGP]: {
        preVisitor: () => ({ continue: false }),
        visitor: (pattern) => {
          bgps.push(pattern)
        },
      },
    });

    // The shapes are fetched once, as every chain below is matched against the same set
    const shapedResources = (await Promise.all(action.resources.map(async resource => ({
      resource,
      shape: await resource.querySource.getSelectorShape(action.context),
    }))))
      .filter((shaped): shaped is IShapedResource => shaped.shape.type === 'operation');

    const usableResources = new Set<IDerivedResource>();
    const patternsToResource = new Map<Algebra.Pattern[], IDerivedResource>();
    for (const bgp of bgps) {
      for (const chain of this.extractLinearSubqueries(bgp.patterns)) {

        // Chains of length 1 are triple patterns which are handled separately.
        // If splitChain is not set and chain length is larger than max we also can't use
        if (chain.length < 2) {
          continue;
        }

        if (chain.length > this.maxChainLength){
          // TODO: Chain splitting done here, we give all options for split chains which
          // are then selected by the partition actor

 
        }

        // A resource answers a path of exactly its own length, so the chain is only pushed down
        // when a resource matches it as a whole. We don't consider cutting up the chain if
        // a derived resource cannot answer the full chain
        const chainBgp = this.algebraFactory.createBgp(chain);
        const match = shapedResources.find(({ shape }) => canAnswerBgp(
          shape,
          chainBgp,
          shape.variablesOptional ?? [],
          shape.variablesRequired ?? [],
        ));
        if (!match) {
          continue;
        }

        usableResources.add(match.resource);
        patternsToResource.set(chain, match.resource);
      }
    }
    
    return {
      candidateResources: []
    };
  }

  protected extractLinearSubqueries(patterns: Algebra.Pattern[]): Algebra.Pattern[][] {
    const candidates = patterns.filter(pattern => pattern.predicate.termType === 'NamedNode');
  
    const bySubject = new Map<string, Algebra.Pattern[]>();
    const byObject = new Map<string, Algebra.Pattern[]>();
    for (const pattern of candidates) {
      this.addToIndex(bySubject, this.termKey(pattern.subject), pattern);
      this.addToIndex(byObject, this.termKey(pattern.object), pattern);
    }
  
    /**
     * The pattern continuing the path at `term`, if `term` links exactly two patterns.
     * Returns undefined when the path ends at `term`, and when it branches there.
     */
    const continuationAt = (term: RDF.Term): Algebra.Pattern | undefined => {
      const key = this.termKey(term);
      const subjects = bySubject.get(key);
      if (subjects?.length !== 1 || byObject.get(key)?.length !== 1) {
        return undefined;
      }
      return subjects[0];
    };
  
    const chains: Algebra.Pattern[][] = [];
    const visited = new Set<Algebra.Pattern>();
  
    for (const candidate of candidates) {
      // Chains are only started at patterns that do not themselves continue another pattern, so
      // that every chain is walked from its start and comes out maximal
      if (continuationAt(candidate.subject) !== undefined) {
        continue;
      }
  
      const chain: Algebra.Pattern[] = [];
      let current: Algebra.Pattern | undefined = candidate;
      while (current && !visited.has(current)) {
        visited.add(current);
        chain.push(current);
        current = continuationAt(current.object);
      }
      chains.push(chain);
    }
  
    return chains;
  }

  protected enumerateSubChains(chain: Algebra.Pattern[], maxChainLength: number){
    // We want to split the chain into parts of length maxChainlength
    const leftOverSize = chain.length - 
      Math.floor(chain.length / maxChainLength) * maxChainLength;
    
    const possibleSplits: number[][][] = [];

    // Try to place leftovers in the chain
    function recurseLeftOver(leftOverTriples: number[]){
      leftOverTriples = [ ... leftOverTriples ];
      // Last added is always last in array and largest index. Previous indexes
      // have already been verified to be plausible
      const lastAddedIndex = leftOverTriples[leftOverTriples.length - 1];

      // Check if admits a complete solution
      if (leftOverTriples.length === leftOverSize){
        if ((chain.length - (lastAddedIndex+1)) % maxChainLength === 0){
          const splitChain: number[][] = [];
          // Split chain while skipping leftOver indexes. Here all
          // chain parts are divisible by maxChainLength so this is safe
          let chainTile: number[] = [];
          for (let k = 0; k < chain.length; k++){
            // Is a leftover
            if (leftOverTriples.includes(k)){
              continue;
            }
            chainTile.push(k);
            if (chainTile.length === maxChainLength){
              splitChain.push(chainTile)
              chainTile = [];
            }
          }

          // Group leftOver indexes if sequential
          let previous = -2;
          let sequentialLeftOver: number[] = [];
          for (const leftOverIndex of leftOverTriples){
            if (leftOverIndex - previous === 1 || sequentialLeftOver.length === 0){
              sequentialLeftOver.push(leftOverIndex)
            }
            else {
              splitChain.push(sequentialLeftOver);
              sequentialLeftOver = [ leftOverIndex ];
            }
            previous = leftOverIndex;
          }

          if (sequentialLeftOver.length > 0){
            splitChain.push(sequentialLeftOver);
          }

          // Here is where we add the split to the accumulating array
          possibleSplits.push(splitChain)
        }
        return;
      }

      for (let i = lastAddedIndex + 1; i < chain.length; i += maxChainLength) {
        const currLeftOver = [ ...leftOverTriples ];
        currLeftOver.push(i);
        recurseLeftOver(currLeftOver);
      }
    }
    for (let i = 0; i < chain.length; i++){
      const leftOverTriples: number[] = [ i ];
      recurseLeftOver(leftOverTriples);
    }
    // for (let i = 0; i < chain.length; i++){
    //   const leftOverTriples: number[] = [ i ];


    //   while (true){
    //     if (leftOverTriples.length === leftOverSize){
    //       // If the rest of the chain is divisible by maxChainLength this is a
    //       // valid configuration
    //       let invalidLeftOver = false;
    //       let previousIdx: number | undefined = undefined;
    //       for (const leftOverIdx of leftOverTriples){
    //         const cutOff = previousIdx ? (previousIdx + 1) : 0
    //         // If the section from previousIdx to leftOverIdx is not divisible by 3
    //         // invalid leftOverTriples
    //         if ((leftOverIdx - cutOff) % maxChainLength !== 0){
    //           invalidLeftOver = true;
    //           break;
    //         }
    //         previousIdx = leftOverIdx;
    //       }
    //       if (invalidLeftOver){
    //         // SKIP THIS LEFTOVER THIS SKIPS ENTIRE ITERATION
    //         // break;
    //       }
    //       if ((chain.length - (previousIdx!+1)) % maxChainLength === 0){
    //         // Valid! We cut up the chain
    //       }
          
    //       // if ((chain.length - (i+1)) % maxChainLength === 0){
    //       //   const splitChain: number[][] = [];
    //       //   // Add chain by splitting into equal parts. This works
    //       //   // due to the preceding check
    //       //   let chainTile: number[] = [];
    //       //   for (let k = 0; k < chain.length; k++){
    //       //     if (k === i){
    //       //       continue;
    //       //     }
    //       //     chainTile.push(k);
    //       //     if (chainTile.length === maxChainLength){
    //       //       splitChain.push(chainTile)
    //       //       chainTile = [];
    //       //     }
    //       //   }
    //       //   splitChain.push([ i ])
    //       //   possibleSplits.push(splitChain)
    //       // }
    //       // break;
    //     }
    //     // Here we add to the leftOverTriples with indexes and check if they're valid

    //   }
    //   // Special case of size = 1
    //   if (leftOverTriples.length === leftOverSize){
    //     // If the rest of the chain is divisible by maxChainLength this is a
    //     // valid configuration
    //     if ((chain.length - (i+1)) % maxChainLength === 0){
    //       const splitChain: number[][] = [];
    //       // Add chain by splitting into equal parts. This works
    //       // due to the preceding check
    //       let chainTile: number[] = [];
    //       for (let k = 0; k < chain.length; k++){
    //         if (k === i){
    //           continue;
    //         }
    //         chainTile.push(k);
    //         if (chainTile.length === maxChainLength){
    //           splitChain.push(chainTile)
    //           chainTile = [];
    //         }
    //       }
    //       splitChain.push([ i ])
    //       possibleSplits.push(splitChain)
    //     }
    //   }
    //   for (let j = i+1; j < chain.length; j++){
    //     // Add to leftover triples
    //     leftOverTriples.push(j);
    //     // Check between the leftover triples if its in blocks of maxChainlength
        
    //   }
    // }
    console.log(possibleSplits)
    return possibleSplits
  }

  private termKey(term: RDF.Term): string {
    return `${term.termType}:${term.value}`;
  }
  
  private addToIndex(index: Map<string, Algebra.Pattern[]>, key: string, pattern: Algebra.Pattern): void {
    const patterns = index.get(key);
    if (patterns) {
      patterns.push(pattern);
    } else {
      index.set(key, [ pattern ]);
    }
  }
  
}


interface IShapedResource {
  resource: IDerivedResource;
  shape: Extract<FragmentSelectorShape, { type: 'operation' }>;
}


export interface IActorDerivedResourceProposeChainArgs extends IActorDerivedResourceProposeArgs{
  maxChainLength: number;
}