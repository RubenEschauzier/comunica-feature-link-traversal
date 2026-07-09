import { createHash } from 'node:crypto';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import type { IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysInitQuery, KeysStatistics } from '@comunica/context-entries';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import { StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';
import { StatisticWriteToFileOverwrite } from '@comunica/statistic-write-to-file-overwrite';

/**
 * A comunica Set Graph Tracking Context Preprocess Actor.
 */
export class ActorContextPreprocessSetTopologyTracking extends ActorContextPreprocess {
  public directoryTraversedTopology: string;

  // Class attributes to persist state across subcalls
  private currentQueryString?: string;
  private currentDiscovery?: StatisticLinkDiscovery;
  private currentDereference?: StatisticLinkDereference;
  private currentTraversedTopology?: StatisticTraversalTopology;
  private currentWriter?: StatisticWriteToFileOverwrite<any>;

  public constructor(args: IActorContextPreprocessSetGraphTrackingArgs) {
    super(args);
    this.directoryTraversedTopology = args.directoryTraversedTopology;
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
    let context = action.context;

    const queryString = context.get(KeysInitQuery.queryString);

    // If a new query string is detected, initialize new trackers
    if (queryString && queryString !== this.currentQueryString) {
      this.currentQueryString = queryString;

      this.currentDiscovery = new StatisticLinkDiscovery();
      this.currentDereference = new StatisticLinkDereference();
      this.currentTraversedTopology = new StatisticTraversalTopology(
        this.currentDiscovery,
        this.currentDereference,
      );

      await fs.mkdir(this.directoryTraversedTopology, { recursive: true });

      // Generate a full hash to prevent collisions
      const queryHash = createHash('md5').update(queryString).digest('hex');
      const timestamp = Date.now();
      const dynamicFilename = path.join(
        this.directoryTraversedTopology,
        `${timestamp}-${queryHash}.json`,
      );

      // Instantiating the writer attaches it as a listener to the topology statistic
      this.currentWriter = new StatisticWriteToFileOverwrite(
        dynamicFilename,
        this.currentTraversedTopology,
      );
    }

    // Ensure trackers exist (failsafe in case a subcall precedes any main query)
    if (!this.currentDiscovery || !this.currentDereference || !this.currentTraversedTopology) {
      throw new Error('Trackers not initialized. A main query must be processed before subcalls.');
    }

    // Always inject the active class trackers into the context for this specific execution step
    context = context.set(KeysStatistics.discoveredLinks, this.currentDiscovery);
    context = context.set(KeysStatistics.dereferencedLinks, this.currentDereference);
    context = context.set(KeysStatisticsTraversal.traversalTopology, this.currentTraversedTopology);

    return { context };
  }
}

export interface IActorContextPreprocessSetGraphTrackingArgs
  extends IActorContextPreprocessArgs {
  /**
   * What directory the topology should be written to
   */
  directoryTraversedTopology: string;
}
