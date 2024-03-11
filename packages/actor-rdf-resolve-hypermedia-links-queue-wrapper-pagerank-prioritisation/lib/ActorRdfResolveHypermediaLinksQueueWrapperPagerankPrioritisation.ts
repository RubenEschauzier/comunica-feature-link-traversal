import { ActorRdfResolveHypermediaLinksQueue, IActionRdfResolveHypermediaLinksQueue, IActorRdfResolveHypermediaLinksQueueOutput, IActorRdfResolveHypermediaLinksQueueArgs } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { IActorArgs, IActorTest } from '@comunica/core';
import { ActionContext, ActionContextKey, Actor, Mediator } from '@comunica/core';
import { MediatorConstructTraversedTopology } from '@comunica/bus-construct-traversed-topology';
import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { LinkQueuePagerankPrioritisation } from './LinkQueuePagerankPrioritisation';

/**
 * A comunica Wrapper Pagerank Prioritisation RDF Resolve Hypermedia Links Queue Actor.
 */
export class ActorRdfResolveHypermediaLinksQueueWrapperPagerankPrioritisation extends ActorRdfResolveHypermediaLinksQueue {
  private readonly mediatorRdfResolveHypermediaLinksQueue: Mediator<
  Actor<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>,
  IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>;
  public readonly mediatorConstructTraversedTopology: MediatorConstructTraversedTopology;

  public constructor(args: IActorRdfResolveHypermediaLinksQueueWrapperPageRankPriotisationArgs) {
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
    console.log("RUN!")
    const { linkQueue } = await this.mediatorRdfResolveHypermediaLinksQueue.mediate({ ...action, context });
    // Empty action to get trackedTopology object to give to the prioritisation wrapper.
    const trackedTopology = await this.mediatorConstructTraversedTopology.mediate( { parentUrl: "",
      links: [],
      metadata: [{}],
      setDereferenced: false,
      context: new ActionContext()
    });

    if (! (linkQueue instanceof LinkQueuePriority)){
      throw new Error("Tried to wrap a non-priority queue with a link prioritisation wrapper.")
    }

    return { linkQueue: new LinkQueuePagerankPrioritisation(linkQueue, trackedTopology.topology) };
  }}



export const KEY_CONTEXT_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-prioritisation:wrapped',
);

export interface IActorRdfResolveHypermediaLinksQueueWrapperPageRankPriotisationArgs
  extends IActorArgs<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput> {
  mediatorRdfResolveHypermediaLinksQueue: Mediator<
  Actor<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>,
  IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>;
  mediatorConstructTraversedTopology: MediatorConstructTraversedTopology
}

