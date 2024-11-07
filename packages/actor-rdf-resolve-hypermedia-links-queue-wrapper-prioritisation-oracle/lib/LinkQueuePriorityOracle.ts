import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';

/**
 * A link queue that only allows the given number of links to be pushed into it.
 */
export class LinkQueuePriorityOracle extends LinkQueueWrapper {
  public readonly RccScores: Record<string, number>;

  public constructor(linkQueue: ILinkQueue, RccScores: Record<string, number>) {
    super(linkQueue);
    this.RccScores = RccScores;
  }

  public override push(link: ILink, parent: ILink): boolean {
    link.metadata = {
      ...link.metadata,
      priority: this.RccScores[link.url] === undefined ? 0 : this.RccScores[link.url],
    };

    return super.push(link, parent);
  }
}
