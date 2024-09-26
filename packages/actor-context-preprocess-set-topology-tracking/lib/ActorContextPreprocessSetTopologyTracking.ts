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
import { ITopologyUpdate, StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';
import { StatisticWriteToFile } from '@comunica/statistic-write-to-file';

/**
 * A comunica Set Topology Tracking Context Preprocess Actor.
 */
export class ActorContextPreprocessSetTopologyTracking extends ActorContextPreprocess {
  public logLocation: string;
  public constructor(args: IActorContextPreprocessTopologyTrackingArgs) {
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
    const topologyLogger = new StatisticWriteToFile<ITopologyUpdate>(
      this.logLocation, 
      statisticTraversalTopology
    );
    action.context = action.context
      .setDefault(KeysStatistics.discoveredLinks, statisticLinkDiscovery)
      .setDefault(KeysStatistics.dereferencedLinks, statisticLinkDereference)
      .setDefault(KeysStatisticsTraversal.traversalTopology, statisticTraversalTopology)
      .setDefault(KeysStatisticsTraversal.writeToFile, topologyLogger);
    return { context: action.context };
  }
}

export interface IActorContextPreprocessTopologyTrackingArgs extends IActorContextPreprocessArgs{
  /**
   * Where topology will be logged to
   */
  logLocation: string
}