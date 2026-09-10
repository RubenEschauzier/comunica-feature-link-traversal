import { ActorDerivedResourcePartition, IActionDerivedResourcePartition, IActorDerivedResourcePartitionOutput, IActorDerivedResourcePartitionArgs, IActorDerivedResourcePartitionTest } from '@comunica/bus-derived-resource-partition';
import { TestResult, passTest } from '@comunica/core';

/**
 * A comunica Star First Derived Resource Partition Actor.
 */
export class ActorDerivedResourcePartitionStarFirst extends ActorDerivedResourcePartition {
  public constructor(args: IActorDerivedResourcePartitionArgs) {
    super(args);
  }

  public async test(action: IActionDerivedResourcePartition): Promise<TestResult<IActorDerivedResourcePartitionTest>> {
    return passTest({ accuracy: .5 });
  }

  public async run(action: IActionDerivedResourcePartition): Promise<IActorDerivedResourcePartitionOutput> {
    const proposedResources = await this.mediatorDerivedResourcePropose.mediate(action);
    return {
      resourceExecutionBlocks: []
    }; 
  }
}



