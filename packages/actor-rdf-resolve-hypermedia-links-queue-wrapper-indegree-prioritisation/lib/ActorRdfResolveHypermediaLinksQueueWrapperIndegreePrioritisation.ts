import { ActorRdfResolveHypermediaLinksQueue, IActionRdfResolveHypermediaLinksQueue, IActorRdfResolveHypermediaLinksQueueOutput, IActorRdfResolveHypermediaLinksQueueArgs } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActionContextKey, Actor, IActorArgs, IActorTest, Mediator } from '@comunica/core';
import { LinkQueueIndegreePrioritisation } from './LinkQueueIndegreePrioritisation';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { StatisticTraversalTopology } from '@comunica/statistic-traversal-topology/lib';

/**
 * A comunica Wrapper Indegree Prioritisation RDF Resolve Hypermedia Links Queue Actor.
 */
export class ActorRdfResolveHypermediaLinksQueueWrapperIndegreePrioritisation extends ActorRdfResolveHypermediaLinksQueue {
  private readonly mediatorRdfResolveHypermediaLinksQueue: Mediator<
  Actor<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>,
  IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>;

  public constructor(args: IActorRdfResolveHypermediaLinksQueueWrapperIndegreePrioritisationArgs) {
    super(args);
  }

  public async test(action: IActionRdfResolveHypermediaLinksQueue): Promise<IActorTest> {
    if (action.context.get(KEY_CONTEXT_WRAPPED)) {
      throw new Error('Unable to wrap link queues multiple times with priority queue');
    }
    return true;
  }

  public async run(action: IActionRdfResolveHypermediaLinksQueue): Promise<IActorRdfResolveHypermediaLinksQueueOutput> {
    const context = action.context.set(KEY_CONTEXT_WRAPPED, true);
    
    const topologyStatistic: StatisticTraversalTopology = action.context.getSafe(
      KeysStatisticsTraversal.traversalTopology
    );

    const { linkQueue } = await this.mediatorRdfResolveHypermediaLinksQueue.mediate({ ...action, context });

    if (! (linkQueue instanceof LinkQueuePriority)){
      throw new Error("Tried to wrap a non-priority queue with a link prioritisation wrapper.")
    }
    return { linkQueue: new LinkQueueIndegreePrioritisation(linkQueue, topologyStatistic) };
  }
}

export const KEY_CONTEXT_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-prioritisation:wrapped',
);

export interface IActorRdfResolveHypermediaLinksQueueWrapperIndegreePrioritisationArgs
  extends IActorArgs<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput> {
  mediatorRdfResolveHypermediaLinksQueue: Mediator<
  Actor<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>,
  IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>;
  mediatorConstructTraversedTopology: MediatorConstructTraversedTopology
}

