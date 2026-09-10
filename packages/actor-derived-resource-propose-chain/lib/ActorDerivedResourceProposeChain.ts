import { IDerivedResource } from '@comunica/actor-extract-links-solid-derived-resources';
import { ActorDerivedResourcePropose, IActionDerivedResourcePropose, IActorDerivedResourceProposeOutput, IActorDerivedResourceProposeArgs } from '@comunica/bus-derived-resource-propose';
import { TestResult, IActorTest, passTestVoid } from '@comunica/core';
import { Algebra, algebraUtils } from '@comunica/utils-algebra';

/**
 * A comunica Chain Derived Resource Propose Actor.
 */
export class ActorDerivedResourceProposeChain extends ActorDerivedResourcePropose {
  public constructor(args: IActorDerivedResourceProposeArgs) {
    super(args);
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
      for (const chain of extractLinearSubqueries(bgp.patterns)) {
        // Chains of length 1 are triple patterns which are handled separately.
        // If splitChain is not set and chain length is larger than max we also can't use
        if (chain.length < 2 || (chain.length > this.maxChainLength && !this.splitChain)) {
          continue;
        }

        if (chain.length > this.maxChainLength && this.splitChain){
          
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
    
    return true; // TODO implement
  }
}
