import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';

/**
 * A link queue that only allows the given number of links to be pushed into it.
 */
export class LinkQueueDepthFirstPrioritization extends LinkQueueWrapper {
  public readonly linksPreviouslyAdded: Record<string, number>;

  public constructor(linkQueue: ILinkQueue) {
    super(linkQueue);
    this.linksPreviouslyAdded = {};
  }

  public override push(link: ILink, parent: ILink): boolean {
    // Seed URLs priority 0, deeper layers have priority parent + 1

    const priority = this.linksPreviouslyAdded[parent.url] ===
    undefined ?
      0 :
      this.linksPreviouslyAdded[parent.url] + 1;
    this.linksPreviouslyAdded[link.url] = priority;
    link.metadata = { ...link.metadata, priority };
    return super.push(link, parent);
  }
}
