import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { ILinkQueue, ILink } from '@comunica/types';

/**
 * A link queue wrapper that filters away links.
 */
export class LinkQueueWrapperFilterDynamic extends LinkQueueWrapper {
  private readonly filter: Set<string>;

  public constructor(linkQueue: ILinkQueue, filterDynamic: Set<string>) {
    super(linkQueue);
    this.filter = filterDynamic;
  }

  public override pop(): ILink | undefined {
    let link = super.pop();
    while (link) {
      if (this.filter.has(link.url)){
        console.log(`Filtered ${link.url}`);
        link = super.pop();
      } else {
        break;
      }
    }
    return link;
  }
}
