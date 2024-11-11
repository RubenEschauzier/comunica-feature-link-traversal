import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';

/**
 * A link queue that only allows the given number of links to be pushed into it.
 */
export class LinkQueueBreadthFirstPrioritization extends LinkQueueWrapper {
  public readonly linksPreviouslyAdded: Record<string, number>;

  public constructor(linkQueue: ILinkQueue) {
    super(linkQueue);
    this.linksPreviouslyAdded = {};
  }

  public override push(link: ILink, parent: ILink): boolean {
    // Priority is 0 for seed URL, which will have no parent in previously pushed nodes
    // Priority is decreased by 1 for each jump away from seed URL
    const priority = (this.linksPreviouslyAdded[parent.url] ?? 1) - 1;
    this.linksPreviouslyAdded[link.url] = priority;

    link.metadata = { ...link.metadata, priority };
    return super.push(link, parent);
  }
}
