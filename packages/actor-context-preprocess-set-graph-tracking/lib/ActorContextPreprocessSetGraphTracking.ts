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
    let context = action.context
    let discovery = <StatisticLinkDiscovery> action.context.get(KeysStatistics.discoveredLinks); 
    if (!discovery){
      discovery = new StatisticLinkDiscovery();
      context = action.context.set(KeysStatistics.discoveredLinks, discovery);   
    }
    let dereference = <StatisticLinkDereference> action.context.get(KeysStatistics.dereferencedLinks); 
    if (!dereference){
      dereference = new StatisticLinkDereference();
      context = context.set(KeysStatistics.dereferencedLinks, dereference);
    }
    let traversedTopology = <StatisticTraversalTopology> 
      action.context.get(KeysStatisticsTraversal.traversalTopology); 
    if (!traversedTopology){
      traversedTopology = new StatisticTraversalTopology(discovery, dereference);
      context = context.set(KeysStatisticsTraversal.traversalTopology, traversedTopology);
    }
    return { context };
  }
}
