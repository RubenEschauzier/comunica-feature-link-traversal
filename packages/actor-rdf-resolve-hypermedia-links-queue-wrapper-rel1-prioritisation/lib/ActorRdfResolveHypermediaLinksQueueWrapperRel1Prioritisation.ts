import { AdjacencyListGraphRcc } from '@comunica/actor-construct-traversed-topology-graph-based-prioritisation';
import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { MediatorConstructTraversedTopology } from '@comunica/bus-construct-traversed-topology';
import { ActorRdfResolveHypermediaLinksQueue, IActionRdfResolveHypermediaLinksQueue, IActorRdfResolveHypermediaLinksQueueOutput } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActionContext, ActionContextKey, Actor, IActorArgs, IActorTest, Mediator } from '@comunica/core';
import { LinkQueueRel1Prioritisation } from './LinkQueueRel1Prioritisation';

/**
 * A comunica Wrapper Rel1 Prioritisation RDF Resolve Hypermedia Links Queue Actor.
 */
export class ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritisation extends ActorRdfResolveHypermediaLinksQueue {
    private readonly mediatorRdfResolveHypermediaLinksQueue: Mediator<
  Actor<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>,
  IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>;
  public readonly mediatorConstructTraversedTopology: MediatorConstructTraversedTopology;

  public constructor(args: IActorRdfResolveHypermediaLinksQueueWrapperRel1PrioritisationArgs) {
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
    return { linkQueue: new LinkQueueRel1Prioritisation(linkQueue, <AdjacencyListGraphRcc> trackedTopology.topology) };
  }
}


export const KEY_CONTEXT_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-prioritisation:wrapped',
);


export interface IActorRdfResolveHypermediaLinksQueueWrapperRel1PrioritisationArgs
  extends IActorArgs<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput> {
  mediatorRdfResolveHypermediaLinksQueue: Mediator<
  Actor<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>,
  IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>;
  mediatorConstructTraversedTopology: MediatorConstructTraversedTopology
}

