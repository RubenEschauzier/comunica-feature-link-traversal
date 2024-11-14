import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import type { IActionRdfResolveHypermediaLinksQueue, IActorRdfResolveHypermediaLinksQueueOutput, MediatorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { KeysStatistics } from '@comunica/context-entries';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { IActorArgs, IActorTest, TestResult } from '@comunica/core';
import { ActionContextKey, failTest, passTestVoid } from '@comunica/core';
import type { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';
import type { StatisticTraversalTopologyRcc } from '@comunica/statistic-traversal-topology-rcc';
import { LinkQueueIsRcc2Prioritization } from './LinkQueueIsRcc2Prioritization';

/**
 * A comunica Wrapper Is Rcc2 Prioritization RDF Resolve Hypermedia Links Queue Actor.
 */
export class ActorRdfResolveHypermediaLinksQueueWrapperIsRcc2Prioritization extends ActorRdfResolveHypermediaLinksQueue {
  private readonly mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;

  public constructor(args: IActorRdfResolveHypermediaLinksQueueWrapperIsRcc2PrioritizationArgs) {
    super(args);
  }

  public async test(action: IActionRdfResolveHypermediaLinksQueue): Promise<TestResult<IActorTest>> {
    if (action.context.get(KEY_CONTEXT_WRAPPED)) {
      return failTest('Unable to wrap link queues multiple times');
    }
    return passTestVoid();
  }

  public async run(action: IActionRdfResolveHypermediaLinksQueue): Promise<IActorRdfResolveHypermediaLinksQueueOutput> {
    const context = action.context.set(KEY_CONTEXT_WRAPPED, true);

    const topologyStatistic = <StatisticTraversalTopologyRcc>
      action.context.getSafe(
        KeysStatisticsTraversal.traversalTopologyRcc,
      );
    const intermediateResults = <StatisticIntermediateResults> action.context.getSafe(
      KeysStatistics.intermediateResults,
    );

    const { linkQueue } = await this.mediatorRdfResolveHypermediaLinksQueue.mediate({ ...action, context });

    if (!(linkQueue instanceof LinkQueuePriority)) {
      throw new TypeError('Tried to wrap a non-priority queue with a link prioritisation wrapper.');
    }

    return {
      linkQueue: new LinkQueueIsRcc2Prioritization(linkQueue, topologyStatistic, intermediateResults),
    };
  }
}

export const KEY_CONTEXT_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-prioritisation:wrapped',
);

export interface IActorRdfResolveHypermediaLinksQueueWrapperIsRcc2PrioritizationArgs
  extends IActorArgs<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput> {
  mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
}
