import { ActorContextPreprocess, IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { IActorArgs, IActorTest } from '@comunica/core';

/**
 * A comunica Set Graph Tracking Context Preprocess Actor.
 */
export class ActorContextPreprocessSetGraphTracking extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(action: IActionContextPreprocess): Promise<IActorTest> {
    return true; // TODO implement
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    return true; // TODO implement
  }
}
