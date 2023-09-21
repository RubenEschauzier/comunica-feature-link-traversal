import type {
  IActionRdfResolveHypermediaLinksQueue,
  IActorRdfResolveHypermediaLinksQueueOutput,
} from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { type Actor, type IActorArgs, type IActorTest, type Mediator } from '@comunica/core';
import { LinkQueuePriorityMetadata } from './LinkQueuePriorityMetadata';

/**
 * A comunica Wrapper Limit Count RDF Resolve Hypermedia Links Queue Actor.
 */
export class ActorRdfResolveHypermediaLinksQueueSolidLinkPrioritisation extends ActorRdfResolveHypermediaLinksQueue {
  private readonly mediatorRdfResolveHypermediaLinksQueue: Mediator<
  Actor<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>,
  IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>;

  public possibleLinkSources: string[];
  public logFileQueueEvolution: string;
  public usePriority: boolean;

  public constructor(args: IActionRdfResolveHypermediaLinksQueueSolidLinkPrioritisationArgs) {
    super(args);
    this.possibleLinkSources = args.possibleLinkSources;
    this.logFileQueueEvolution = args.logFileQueueEvolution;
    this.usePriority = args.usePriority;
  }

  public async test(action: IActionRdfResolveHypermediaLinksQueue): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionRdfResolveHypermediaLinksQueue): Promise<IActorRdfResolveHypermediaLinksQueueOutput> {
    return { linkQueue: new LinkQueuePriorityMetadata(this.possibleLinkSources, this.logFileQueueEvolution, this.usePriority) };
  }
}

export interface IActionRdfResolveHypermediaLinksQueueSolidLinkPrioritisationArgs
  extends IActorArgs<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput> {
  possibleLinkSources: string[];
  logFileQueueEvolution: string;
  usePriority: boolean;
  mediatorRdfResolveHypermediaLinksQueue: Mediator<
  Actor<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>,
  IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>;
}

