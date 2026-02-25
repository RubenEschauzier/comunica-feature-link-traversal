import { ActorRdfResolveHypermediaLinksQueue, IActionRdfResolveHypermediaLinksQueue, IActorRdfResolveHypermediaLinksQueueOutput, IActorRdfResolveHypermediaLinksQueueArgs, MediatorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { TestResult, IActorTest, passTestVoid, failTest, ActionContextKey } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperFilter } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-filter';
import { KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { LinkQueueWrapperFilterDynamic } from './LinkQueueWrapperFilterDynamic';

/**
 * A comunica Wrapper Filter Dynamic RDF Resolve Hypermedia Links Queue Actor.
 */
export class ActorRdfResolveHypermediaLinksQueueWrapperFilterDynamic extends ActorRdfResolveHypermediaLinksQueue {
  private readonly mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;

  private static readonly keyWrapped = new ActionContextKey<boolean>(
    '@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-filter-dynamic:wrapped',
  );

  public constructor(args: IActorRdfResolveHypermediaLinksQueueWrapperFilterDynamicArgs) {
    super(args);
    this.mediatorRdfResolveHypermediaLinksQueue = args.mediatorRdfResolveHypermediaLinksQueue;
  }

  public async test(action: IActionRdfResolveHypermediaLinksQueue): Promise<TestResult<IActorTest>> {
    if (action.context.get(ActorRdfResolveHypermediaLinksQueueWrapperFilterDynamic.keyWrapped)) {
      return failTest('Unable to wrap link queues multiple times');
    }
    if (!action.context.has(KeysRdfResolveHypermediaLinks.dynamicFilter)) {
      return failTest('Unable to wrap link queue with missing dynamic filter');
    }
    return passTestVoid();
  }

  public async run(action: IActionRdfResolveHypermediaLinksQueue): Promise<IActorRdfResolveHypermediaLinksQueueOutput> {
    const context = action.context.set(ActorRdfResolveHypermediaLinksQueueWrapperFilterDynamic.keyWrapped, true);
    const dynamicFilter = action.context.getSafe(KeysRdfResolveHypermediaLinks.dynamicFilter);
    const { linkQueue } = await this.mediatorRdfResolveHypermediaLinksQueue.mediate({ ...action, context });
    return {
      linkQueue: new LinkQueueWrapperFilterDynamic(linkQueue, dynamicFilter),
    };
  }
}

export interface IActorRdfResolveHypermediaLinksQueueWrapperFilterDynamicArgs extends
  IActorRdfResolveHypermediaLinksQueueArgs {
  mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
}
