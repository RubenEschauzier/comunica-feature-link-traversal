import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';

/**
 * A link queue that only allows the given number of links to be pushed into it.
 */
export class LinkQueuePriorityRandom extends LinkQueueWrapper {
  public constructor(linkQueue: ILinkQueue) {
    super(linkQueue);
  }

  public override push(link: ILink, parent: ILink): boolean {
    link.metadata = { ...link.metadata, priority: this.randomInt(0, 10) };
    return super.push(link, parent);
  }

  public randomInt(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }
}
