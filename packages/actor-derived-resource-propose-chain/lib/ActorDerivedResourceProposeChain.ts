import { IDerivedResource } from '@comunica/actor-extract-links-solid-derived-resources';
import { ActorDerivedResourcePropose, IActionDerivedResourcePropose, IActorDerivedResourceProposeOutput, IActorDerivedResourceProposeArgs, ICandidateResource } from '@comunica/bus-derived-resource-propose';
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

    const proposedChainResources: ICandidateResource[] = [];
    for (const bgp of bgps) {
      for (const chain of this.extractLinearSubqueries(bgp.patterns)) {

        // Chains of length 1 are triple patterns which are handled separately
        if (chain.length < 2) {
          continue;
        }

        // A resource answers a path of exactly its own length, so a chain that is too long is
        // only pushed down in parts. Every proposed chunk is offered on its own: any set of
        // chunks that do not overlap is executable, and it is up to the partition actor to
        // decide which combination of these (and of the other proposed resources) to take
        for (const chunk of this.enumerateProposableChains(chain)) {
          const chunkBgp = this.algebraFactory.createBgp(chunk);
          const match = shapedResources.find(({ shape }) => canAnswerBgp(
            shape,
            chunkBgp,
            shape.variablesOptional ?? [],
            shape.variablesRequired ?? [],
          ));
          if (!match) {
            continue;
          }

          proposedChainResources.push(this.createCandidateResource(chunk, match.resource));
        }
      }
    }

    return {
      candidateResources: proposedChainResources
    };
  }

  /**
   * The parts of the chain that are worth matching against a resource.
   */
  protected enumerateProposableChains(chain: Algebra.Pattern[]): Algebra.Pattern[][] {
    if (chain.length <= this.maxChainLength) {
      return [ chain ];
    }

    const proposable: Algebra.Pattern[][] = [];
    const seen = new Set<string>();
    for (const split of this.enumerateSubChains(chain, this.maxChainLength)) {
      for (const part of [ ...split.chunks, ...split.leftOvers ]) {
        const key = `${part[0]}:${part.length}`;
        if (part.length < 2 || seen.has(key)) {
          continue;
        }
        seen.add(key);
        proposable.push(part.map(index => chain[index]));
      }
    }
    return proposable;
  }

  protected createCandidateResource(chunk: Algebra.Pattern[], resource: IDerivedResource): ICandidateResource {
    const nodes = [ chunk[0].subject, ...chunk.map(pattern => pattern.object) ];
    const constants = new Set(nodes
      .filter(node => node.termType !== 'Variable')
      .map(node => this.termKey(node)));

    return {
      operations: chunk,
      resource,
      kind: 'chain',
      anchorTerms: chunk.map(pattern => pattern.subject),
      prunable: true,
      features: {
        patternCount: chunk.length,
        constantCount: constants.size,
        // TODO: these are the placeholder values the star actor uses as well
        coefficients: {
          compute: 5,
          requests: 1,
          selectivity: 5
        }
      }
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

  protected enumerateSubChains(chain: Algebra.Pattern[], maxChainLength: number): ISubChainSplit[] {
    // We want to split the chain into parts of length maxChainlength
    const leftOverSize = chain.length % maxChainLength;
    const possibleSplits: ISubChainSplit[] = [];

    // Try to place leftovers in the chain
    function recurseLeftOver(leftOverTriples: number[]){
      // Last added is always last in array and largest index. Previous indexes
      // have already been verified to be plausible. Without any leftovers yet the
      // chain is still untouched, so the next candidate starts at 0
      const lastAddedIndex = leftOverTriples.length > 0 ?
        leftOverTriples[leftOverTriples.length - 1] :
        -1;

      // Check if admits a complete solution
      if (leftOverTriples.length === leftOverSize){
        if ((chain.length - (lastAddedIndex+1)) % maxChainLength === 0){
          const isLeftOver = new Set(leftOverTriples);
          const chunks: number[][] = [];
          // Split chain while skipping leftOver indexes. Here all
          // chain parts are divisible by maxChainLength so this is safe
          let chainTile: number[] = [];
          for (let k = 0; k < chain.length; k++){
            // Is a leftover
            if (isLeftOver.has(k)){
              continue;
            }
            chainTile.push(k);
            if (chainTile.length === maxChainLength){
              chunks.push(chainTile);
              chainTile = [];
            }
          }

          // Group leftOver indexes if sequential, as adjacent leftovers still
          // form a (too short) chain of their own
          const leftOvers: number[][] = [];
          let previous = -2;
          for (const leftOverIndex of leftOverTriples){
            if (leftOverIndex - previous === 1){
              leftOvers[leftOvers.length - 1].push(leftOverIndex);
            }
            else {
              leftOvers.push([ leftOverIndex ]);
            }
            previous = leftOverIndex;
          }

          // Here is where we add the split to the accumulating array
          possibleSplits.push({ chunks, leftOvers });
        }
        return;
      }

      for (let i = lastAddedIndex + 1; i < chain.length; i += maxChainLength) {
        const currLeftOver = [ ...leftOverTriples ];
        currLeftOver.push(i);
        recurseLeftOver(currLeftOver);
      }
    }
    // Starting without leftovers covers chains that need none at all, and the recursion
    // itself only proposes first leftovers at multiples of maxChainLength, which are the
    // only positions that leave a valid prefix
    if (chain.length > 0){
      recurseLeftOver([]);
    }
    return possibleSplits;
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

/**
 * One way of cutting a chain up, as indexes into the chain.
 */
export interface ISubChainSplit {
  /**
   * The parts of exactly maxChainLength, in chain order, that can be pushed down as a chain.
   */
  chunks: number[][];
  /**
   * The parts that are too short to be pushed down as a chain, in chain order.
   * Leftovers that are adjacent in the chain are grouped into a single part.
   */
  leftOvers: number[][];
}


export interface IActorDerivedResourceProposeChainArgs extends IActorDerivedResourceProposeArgs{
  maxChainLength: number;
}