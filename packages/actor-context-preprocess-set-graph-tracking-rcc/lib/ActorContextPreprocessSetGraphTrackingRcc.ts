import type { IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysStatistics } from '@comunica/context-entries';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';
import { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import { StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';
import { StatisticTraversalTopologyRcc } from '@comunica/statistic-traversal-topology-rcc';

/**
 * A comunica Set Graph Tracking Context Preprocess Actor.
 */
export class ActorContextPreprocessSetGraphTrackingRcc extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(_action: IActionContextPreprocess): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    const discovery: StatisticLinkDiscovery = new StatisticLinkDiscovery();
    const dereference: StatisticLinkDereference = new StatisticLinkDereference();
    const intermediateResult: StatisticIntermediateResults = new StatisticIntermediateResults();
    const traversedTopology: StatisticTraversalTopology =
      new StatisticTraversalTopology(discovery, dereference);
    const traversedTopologyRcc = new StatisticTraversalTopologyRcc(traversedTopology, intermediateResult);
    const context = action.context
      .set(KeysStatistics.discoveredLinks, discovery)
      .set(KeysStatistics.dereferencedLinks, dereference)
      .set(KeysStatistics.intermediateResults, intermediateResult)
      .set(KeysStatisticsTraversal.traversalTopology, traversedTopology)
      .set(KeysStatisticsTraversal.traversalTopologyRcc, traversedTopologyRcc);
    return { context };
  }
}
