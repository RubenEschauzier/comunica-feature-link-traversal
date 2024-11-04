import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { ActorRdfResolveHypermediaLinksQueue, IActionRdfResolveHypermediaLinksQueue, IActorRdfResolveHypermediaLinksQueueOutput, IActorRdfResolveHypermediaLinksQueueArgs, MediatorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import { ActionContextKey, Actor, failTest, IActorArgs, IActorTest, Mediator, passTestVoid, TestResult } from '@comunica/core';
import { StatisticTraversalTopologyRcc } from '@comunica/statistic-traversal-topology-rcc';
import { LinkQueueRel1Prioritization } from './LinkQueueRel1Prioritization';

/**
 * A comunica Wrapper Rel1 Prioritization RDF Resolve Hypermedia Links Queue Actor.
 */
export class ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritization extends ActorRdfResolveHypermediaLinksQueue {
  private readonly mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue

  public constructor(args: IActorRdfResolveHypermediaLinksQueueWrapperRel1PrioritizationArgs) {
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

    const topologyStatistic: StatisticTraversalTopologyRcc = <StatisticTraversalTopologyRcc>
      action.context.getSafe(
        KeysStatisticsTraversal.traversalTopologyRcc
      );

    const { linkQueue } = await this.mediatorRdfResolveHypermediaLinksQueue.mediate({ ...action, context });

    if (! (linkQueue instanceof LinkQueuePriority)){
      throw new Error("Tried to wrap a non-priority queue with a link prioritisation wrapper.")
    }

    return { linkQueue: new LinkQueueRel1Prioritization(linkQueue, topologyStatistic) };
  }
}

export const KEY_CONTEXT_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-prioritisation:wrapped',
);

export interface IActorRdfResolveHypermediaLinksQueueWrapperRel1PrioritizationArgs
  extends IActorArgs<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput> {
    mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
  }

