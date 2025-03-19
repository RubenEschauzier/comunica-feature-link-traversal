import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';

/**
 * A link queue that only allows the given number of links to be pushed into it.
 */
export class LinkQueueTypeIndexPrioritization extends LinkQueueWrapper {

  public constructor(linkQueue: ILinkQueue) {
    super(linkQueue);
  }

  public override push(link: ILink, parent: ILink): boolean {
    console.log(link.metadata)
    return super.push(link, parent);
  }
}
