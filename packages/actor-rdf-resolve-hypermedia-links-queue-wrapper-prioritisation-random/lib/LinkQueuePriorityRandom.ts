import type { ILinkPriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';

/**
 * A link queue that only allows the given number of links to be pushed into it.
 */
export class LinkQueuePriorityRandom extends LinkQueueWrapper {
  public constructor(linkQueue: ILinkQueue) {
    super(linkQueue);
  }

  public push(link: ILink, parent: ILink): boolean {
    const randomPriority = this.randomInt(0, 10);

    const linkPriority: ILinkPriority = <ILinkPriority> link;
    linkPriority.priority = randomPriority;

    return super.push(linkPriority, parent);
  }

  private randomInt(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }
}

