import { ActorOptimizeQueryOperation, IActionOptimizeQueryOperation, IActorOptimizeQueryOperationOutput, IActorOptimizeQueryOperationArgs } from '@comunica/bus-optimize-query-operation';
import { TestResult, IActorTest, passTestVoid } from '@comunica/core';

/**
 * A comunica Set Cache Cset Offline Traversal Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheCsetOfflineTraversal extends ActorOptimizeQueryOperation {
  public constructor(args: IActorOptimizeQueryOperationArgs) {
    super(args);
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    return passTestVoid(); // TODO implement
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    return true; // TODO implement
  }
}
