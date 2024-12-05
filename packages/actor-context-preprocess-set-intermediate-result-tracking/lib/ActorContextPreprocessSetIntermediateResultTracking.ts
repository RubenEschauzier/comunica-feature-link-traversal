import type { IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysStatistics } from '@comunica/context-entries';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
import { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';

/**
 * A comunica Set Graph Tracking Context Preprocess Actor.
 */
export class ActorContextPreprocessSetIntermediateResultTracking extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(action: IActionContextPreprocess): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    let context = action.context;

    let intermediateResult = <StatisticIntermediateResults> action.context.get(
      KeysStatistics.intermediateResults,
    );
    // We can disable tracking
    if (!intermediateResult) {
      intermediateResult = new StatisticIntermediateResults();
      context = context.set(KeysStatistics.intermediateResults, intermediateResult);
    }
    return { context };
  }
}
