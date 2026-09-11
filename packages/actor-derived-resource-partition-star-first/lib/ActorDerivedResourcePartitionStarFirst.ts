import { ActorDerivedResourcePartition, IActionDerivedResourcePartition, IActorDerivedResourcePartitionOutput, IActorDerivedResourcePartitionArgs, IActorDerivedResourcePartitionTest, IResourceExecutionBlock } from '@comunica/bus-derived-resource-partition';
import { ICandidateResource } from '@comunica/bus-derived-resource-propose';
import { TestResult, passTest } from '@comunica/core';
import { Algebra, AlgebraFactory } from '@comunica/utils-algebra';

/**
 * A comunica Star First Derived Resource Partition Actor.
 */
export class ActorDerivedResourcePartitionStarFirst extends ActorDerivedResourcePartition {
  protected readonly algebraFactory = new AlgebraFactory();

  public constructor(args: IActorDerivedResourcePartitionArgs) {
    super(args);
  }

  public async test(action: IActionDerivedResourcePartition): Promise<TestResult<IActorDerivedResourcePartitionTest>> {
    return passTest({ accuracy: .5 });
  }

  public async run(action: IActionDerivedResourcePartition): Promise<IActorDerivedResourcePartitionOutput> {
    const proposedResources = await this.mediatorDerivedResourcePropose.mediate(action);

    const ranked = [ ...proposedResources.candidateResources ].sort((left, right) =>
      this.tier(left) - this.tier(right) || this.compare(left, right));

    // The proposers can produce overlapping candidates, so the partition is built by walking
    // them best-first and handing every operation to the first candidate that claims it
    const claimed = new Set<Algebra.Operation>();
    const resourceExecutionBlocks: IResourceExecutionBlock[] = [];
    for (const candidate of ranked) {
      const unclaimed = candidate.operations.filter(operation => !claimed.has(operation));

      // A candidate was matched against its resource as a whole. If parts are claimed
      // the propose actor must once again try to match a resource against the new operation
      let block = candidate;
      if (unclaimed.length < candidate.operations.length) {
        const reduced = await this.retryReducedOperation(action, unclaimed);
        if (!reduced) {
          continue;
        }
        block = reduced;
      }

      for (const operation of block.operations) {
        claimed.add(operation);
      }
      resourceExecutionBlocks.push({
        operation: block.operations,
        resource: block.resource,
        anchors: block.anchorTerms,
        prunable: block.prunable,
      });
    }

    return { resourceExecutionBlocks };
  }

  /**
   * Try to find matching resource for reduced operation
   */
  protected async retryReducedOperation(
    action: IActionDerivedResourcePartition,
    operations: Algebra.Operation[],
  ): Promise<ICandidateResource | undefined> {
    if (operations.length < 2) {
      return undefined;
    }

    const { candidateResources } = await this.mediatorDerivedResourcePropose.mediate({
      ...action,
      operation: this.asSingleOperation(operations),
    });

    return [ ...candidateResources ]
      .filter(candidate => candidate.operations.length > 1)
      .sort((left, right) => this.tier(left) - this.tier(right) || this.compare(left, right))[0];
  }

  protected asSingleOperation(operations: Algebra.Operation[]): Algebra.Operation {
    const patterns = operations.filter((operation): operation is Algebra.Pattern =>
      operation.type === Algebra.Types.PATTERN);

    // A join of patterns is a bgp, and the proposers only take the bgp apart again
    return patterns.length === operations.length ?
      this.algebraFactory.createBgp(patterns) :
      this.algebraFactory.createJoin(operations);
  }

  protected tier(candidate: ICandidateResource): number {
    if (candidate.kind !== 'star') {
      return 1;
    }
    return candidate.operations.length > 1 ? 0 : 2;
  }

  /**
   * Orders candidates of the same tier, best first.
   */
  protected compare(left: ICandidateResource, right: ICandidateResource): number {
    return right.features.patternCount - left.features.patternCount ||
      // A bound term makes the sub-query more selective, so the resource answers with less for the
      // rest of the query to join over, and the block does not wait on another block to bind it
      right.features.constantCount - left.features.constantCount;
  }
}
