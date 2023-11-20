import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';

/**
 * A link queue that only allows the given number of links to be pushed into it.
 */
export class LinkQueuePriorityNonAdaptive extends LinkQueueWrapper {
  public readonly linksPreviouslyAdded: Record<string, number>;
  public constructor(linkQueue: ILinkQueue) {
    super(linkQueue);
  }

  public push(link: ILink, parent: ILink): boolean {
    return super.push(link, parent);
  }
}

