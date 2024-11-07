import type { IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysStatistics } from '@comunica/context-entries';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import { StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';

/**
 * A comunica Set Graph Tracking Context Preprocess Actor.
 */
export class ActorContextPreprocessSetGraphTracking extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(action: IActionContextPreprocess): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    const discovery: StatisticLinkDiscovery = new StatisticLinkDiscovery();
    const dereference: StatisticLinkDereference = new StatisticLinkDereference();
    const traversedTopology: StatisticTraversalTopology =
      new StatisticTraversalTopology(discovery, dereference);
    let context = action.context.set(KeysStatistics.discoveredLinks, discovery);
    context = context.set(KeysStatistics.dereferencedLinks, dereference);
    context = context.set(KeysStatisticsTraversal.traversalTopology, traversedTopology);
    return { context };
  }
}
