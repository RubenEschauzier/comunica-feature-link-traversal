import type { IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import type { IActorTest } from '@comunica/core';

/**
 * A comunica Set Write To File Context Preprocess Actor.
 */
export class ActorContextPreprocessSetWriteToFile extends ActorContextPreprocess {
  public statisticsToWrite: string[];
  public fileLocations: string[];

  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(action: IActionContextPreprocess): Promise<IActorTest> {
    return true; // TODO implement
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    return { context: action.context }; // TODO implement
  }
}

export interface IActorContextPreprocessSetWriteToFile extends IActionContextPreprocess {
  /**
   * Locations to write output of statistics to.
   */
  fileLocations: string[];
  /**
   * What statistics the writer should write to file.
   */
  statisticsToWrite: string[];
}
