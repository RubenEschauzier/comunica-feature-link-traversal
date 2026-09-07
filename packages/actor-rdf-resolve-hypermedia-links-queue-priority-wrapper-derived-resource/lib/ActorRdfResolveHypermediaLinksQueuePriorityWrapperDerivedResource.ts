import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { ActorRdfResolveHypermediaLinksQueue, IActionRdfResolveHypermediaLinksQueue, IActorRdfResolveHypermediaLinksQueueOutput, IActorRdfResolveHypermediaLinksQueueArgs, MediatorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { TestResult, IActorTest, passTestVoid, ActionContextKey, IActorArgs, failTest } from '@comunica/core';
import { LinkQueueDerivedResourcePrioritization } from './LinkQueueDerivedResourcePrioritization';

/**
 * A comunica Priority Derived Resource RDF Resolve Hypermedia Links Queue Actor.
 */
export class ActorRdfResolveHypermediaLinksQueuePriorityWrapperDerivedResource extends ActorRdfResolveHypermediaLinksQueue {
  private readonly mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
  private readonly derivedResourcePredicate: string;

  public constructor(args: IActorRdfResolveHypermediaLinksQueueWrapperPriorityDerivedResourceeArgs) {
    super(args);
    this.mediatorRdfResolveHypermediaLinksQueue = args.mediatorRdfResolveHypermediaLinksQueue;
    this.derivedResourcePredicate = args.derivedResourcePredicate;
  }

  public async test(action: IActionRdfResolveHypermediaLinksQueue): Promise<TestResult<IActorTest>> {
    if (action.context.get(KEY_CONTEXT_WRAPPED)) {
      return failTest('Unable to wrap link queues multiple times');
    }
    
    return passTestVoid();
  }

  public async run(action: IActionRdfResolveHypermediaLinksQueue): Promise<IActorRdfResolveHypermediaLinksQueueOutput> {
    const context = action.context.set(KEY_CONTEXT_WRAPPED, true);

    const { linkQueue } = await this.mediatorRdfResolveHypermediaLinksQueue.mediate({ ...action, context });

    if (!(linkQueue instanceof LinkQueuePriority)) {
      throw new TypeError('Tried to wrap a non-priority queue with a link prioritisation wrapper.');
    }

    return { linkQueue: new LinkQueueDerivedResourcePrioritization(linkQueue, this.derivedResourcePredicate) };  }
}


export const KEY_CONTEXT_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-prioritization-derived-resource:wrapped',
);


export interface IActorRdfResolveHypermediaLinksQueueWrapperPriorityDerivedResourceeArgs
  extends IActorArgs<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput> {
  mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
  derivedResourcePredicate: string;

}