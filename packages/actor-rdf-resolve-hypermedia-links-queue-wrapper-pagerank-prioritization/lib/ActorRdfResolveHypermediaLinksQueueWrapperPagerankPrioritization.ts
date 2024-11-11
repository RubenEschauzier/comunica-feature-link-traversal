import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import type { IActionRdfResolveHypermediaLinksQueue, IActorRdfResolveHypermediaLinksQueueOutput, MediatorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { IActorArgs, IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid, ActionContextKey } from '@comunica/core';
import type { StatisticTraversalTopology } from '@comunica/statistic-traversal-topology/lib';
import { LinkQueuePagerankPrioritization } from './LinkQueuePagerankPrioritization';

/**
 * A comunica Wrapper Pagerank Prioritization RDF Resolve Hypermedia Links Queue Actor.
 */
export class ActorRdfResolveHypermediaLinksQueueWrapperPagerankPrioritization extends ActorRdfResolveHypermediaLinksQueue {
  private readonly mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;

  public constructor(args: IActorRdfResolveHypermediaLinksQueueWrapperPageRankPrioritizationArgs) {
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

    const topologyStatistic: StatisticTraversalTopology = <StatisticTraversalTopology>
      action.context.getSafe(
        KeysStatisticsTraversal.traversalTopology,
      );

    const { linkQueue } = await this.mediatorRdfResolveHypermediaLinksQueue.mediate({ ...action, context });

    if (!(linkQueue instanceof LinkQueuePriority)) {
      throw new TypeError('Tried to wrap a non-priority queue with a link prioritization wrapper.');
    }

    return { linkQueue: new LinkQueuePagerankPrioritization(linkQueue, topologyStatistic) };
  }
}

export const KEY_CONTEXT_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-prioritization:wrapped',
);

export interface IActorRdfResolveHypermediaLinksQueueWrapperPageRankPrioritizationArgs
  extends IActorArgs<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput> {
  mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
}
