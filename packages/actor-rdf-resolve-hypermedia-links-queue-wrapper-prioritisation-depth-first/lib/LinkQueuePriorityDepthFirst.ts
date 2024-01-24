import type { ILinkPriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';

/**
 * A link queue that only allows the given number of links to be pushed into it.
 */
export class LinkQueuePriorityDepthFirst extends LinkQueueWrapper {
  public readonly linksPreviouslyAdded: Record<string, number>;

  public constructor(linkQueue: ILinkQueue) {
    super(linkQueue);
    this.linksPreviouslyAdded = {};
  }

  public push(link: ILink, parent: ILink): boolean {
    // Seed URLs priority 0, deeper layers have priority parent + 1
    const priority = this.linksPreviouslyAdded[parent.url] === undefined ? 0 : this.linksPreviouslyAdded[parent.url] + 1;
    this.linksPreviouslyAdded[link.url] = priority;

    const linkPriority: ILinkPriority = <ILinkPriority> link;
    linkPriority.priority = priority;
    console.log(linkPriority.url);
    console.log(linkPriority.priority);

    return super.push(link, parent);
  }
}

