import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ILinkPriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';

/**
 * A link queue that only allows the given number of links to be pushed into it.
 */
export class LinkQueuePriorityOracle extends LinkQueueWrapper {
  public readonly RccScores: Record<string, number>;

  public constructor(linkQueue: ILinkQueue, RccScores: Record<string, number>) {
    super(linkQueue);
    this.RccScores = RccScores;
  }

  public push(link: ILink, parent: ILink): boolean {
    const linkPriority: ILinkPriority = <ILinkPriority> link;
    linkPriority.priority = this.RccScores[link.url] === undefined ? 0 : this.RccScores[link.url];
    
    return super.push(linkPriority, parent);
  }
}

