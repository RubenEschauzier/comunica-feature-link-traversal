import type { IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysStatistics } from '@comunica/context-entries';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
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
    let context = action.context;
       
    let discovery = <StatisticLinkDiscovery> action.context.get(KeysStatistics.discoveredLinks);
    if (!discovery) {
      discovery = new StatisticLinkDiscovery();
    }
    let dereference = <StatisticLinkDereference> action.context.get(KeysStatistics.dereferencedLinks);
    if (!dereference) {
      dereference = new StatisticLinkDereference();
    }
    let traversedTopology = <StatisticTraversalTopology>
      action.context.get(KeysStatisticsTraversal.traversalTopology);
    if (!traversedTopology) {
      traversedTopology = new StatisticTraversalTopology(discovery, dereference);
    }
    let intermediateResult = <StatisticIntermediateResults> action.context.get(
      KeysStatistics.intermediateResults,
    );
    if (!intermediateResult) {
      intermediateResult = new StatisticIntermediateResults();
    }
    const traversedTopologyRcc = new StatisticTraversalTopologyRcc(traversedTopology, intermediateResult);
    context = action.context
      .set(KeysStatistics.discoveredLinks, discovery)
      .set(KeysStatistics.dereferencedLinks, dereference)
      .set(KeysStatistics.intermediateResults, intermediateResult)
      .set(KeysStatisticsTraversal.traversalTopology, traversedTopology)
      .set(KeysStatisticsTraversal.traversalTopologyRcc, traversedTopologyRcc);
    return { context };
  }
}
