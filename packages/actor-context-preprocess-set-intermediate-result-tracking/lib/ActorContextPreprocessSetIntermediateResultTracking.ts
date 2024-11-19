import type { IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysStatistics } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';

/**
 * A comunica Set Graph Tracking Context Preprocess Actor.
 */
export class ActorContextPreprocessSetIntermediateResultTracking extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(_action: IActionContextPreprocess): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    let context = action.context;
    let intermediateResult = <StatisticIntermediateResults> action.context.get(
      KeysStatistics.intermediateResults
    );
    if (!intermediateResult){
      intermediateResult = new StatisticIntermediateResults();
      context = context.set(KeysStatistics.intermediateResults, intermediateResult)
    }
    return { context };
  }
}
