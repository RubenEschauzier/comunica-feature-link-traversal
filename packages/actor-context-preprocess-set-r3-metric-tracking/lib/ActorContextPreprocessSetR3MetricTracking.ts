import type { IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysInitQuery, KeysStatistics } from '@comunica/context-entries';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import { StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';
import { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';
import { StatisticWriteToFileOverwrite } from '@comunica/statistic-write-to-file-overwrite';
import { StatisticWriteToFile } from '@comunica/statistic-write-to-file';

/**
 * A comunica Set Graph Tracking Context Preprocess Actor.
 */
export class ActorContextPreprocessSetR3MetricTracking extends ActorContextPreprocess {
  public baseDirectoryExperiment: string; 
  public fileLocationBase64toOutputDir: string;
  /**
   * Track what query number we are on (doesn't work when the endpoint is reset.)
   */
  public queryNum: number;
 
  public constructor(args: IActorContextPreprocessSetGraphTrackingArgs) {
    super(args);
  }

  public async test(_action: IActionContextPreprocess): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }
  /**
  * Set the required statistic trackers for R3 metric calculation. Only set if the statistics
  * are not yet available.
  * @param action 
  * @returns 
  */
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
    let intermediateResult = <StatisticIntermediateResults> action.context.get(
      KeysStatistics.intermediateResults
    );
    if (!intermediateResult){
      intermediateResult = new StatisticIntermediateResults();
      context = context.set(KeysStatistics.intermediateResults, intermediateResult)
    }
    // Use overwrite statistic as the entire topology is output as update by traversed
    // topology statistic
    const query = action.context.getSafe(KeysInitQuery.queryString);
    const statisticTrackTopology = new StatisticWriteToFileOverwrite({
      query,
      statisticToWrite: traversedTopology,
      baseDirectoryExperiment: this.baseDirectoryExperiment,
      fileLocationBase64ToDir: this.fileLocationBase64toOutputDir,
    });
    const statisticTrackResults = new StatisticWriteToFile({
      query,
      statisticToWrite: intermediateResult,
      baseDirectoryExperiment: this.baseDirectoryExperiment,
      fileLocationBase64ToDir: this.fileLocationBase64toOutputDir,
    });

    return { context };
  }
}

export interface IActorContextPreprocessSetGraphTrackingArgs 
  extends IActorContextPreprocessArgs {
  /**
   * What directory the current experiment should write to
   */
  baseDirectoryExperiment: string,
  /**
   * What file results should be written to
   */
  fileLocationBase64toOutputDir?: string
}