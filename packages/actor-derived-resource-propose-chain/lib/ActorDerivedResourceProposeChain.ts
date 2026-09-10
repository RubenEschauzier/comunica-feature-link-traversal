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
    
    return true;
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

  protected enumerateChainSplits(chain: Algebra.Pattern[]){
    const lengthChain = chain.length;
    for (let i = 0; i < lengthChain; i++){
      // If section of maxChainLength > length of chain 
      // we do ...
      if (i + this.maxChainLength > lengthChain - 1){

      }
      const chainSection = chain.slice(i, i + this.maxChainLength);
      
    }
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