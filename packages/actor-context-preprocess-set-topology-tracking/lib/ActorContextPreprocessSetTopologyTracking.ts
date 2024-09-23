import type {
  IActionContextPreprocess,
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysStatistics } from '@comunica/context-entries';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { IActorTest } from '@comunica/core';
import { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import { StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';

/**
 * A comunica Set Topology Tracking Context Preprocess Actor.
 */
export class ActorContextPreprocessSetTopologyTracking extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(_action: IActionContextPreprocess): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    const statisticLinkDiscovery = new StatisticLinkDiscovery();
    const statisticLinkDereference = new StatisticLinkDereference();
    const statisticTraversalTopology = new StatisticTraversalTopology(
      statisticLinkDiscovery,
      statisticLinkDereference,
    );
    action.context = action.context
      .setDefault(KeysStatistics.discoveredLinks, statisticLinkDiscovery)
      .setDefault(KeysStatistics.dereferencedLinks, statisticLinkDereference)
      .setDefault(KeysStatisticsTraversal.traversalTopology, statisticTraversalTopology);
    return { context: action.context };
  }
}
