import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { ILinkQueue, ILink } from '@comunica/types';
import { IDynamicFilter } from '@comunica/types-link-traversal';

/**
 * A link queue wrapper that dynamically filters links using a live object 
 * of exact matches and pre-compiled regular expressions.
 *
 * A link the filter is still deciding on is set aside rather than dropped, and queued again once
 * the filter releases it.
 */
export class LinkQueueWrapperFilterDynamic extends LinkQueueWrapper {
  private readonly filter: IDynamicFilter;
  private held: ILink[] = [];

  public constructor(linkQueue: ILinkQueue, filterDynamic: IDynamicFilter) {
    super(linkQueue);
    this.filter = filterDynamic;
    this.filter.addReleaseListener(() => this.requeueReleased());
  }

  public override pop(): ILink | undefined {
    let link = super.pop();
    while (link) {
      if (this.filter.matchesFilter(link.url)) {
        link = super.pop();
      } else if (this.filter.isHeld(link.url)) {
        this.held.push(link);
        link = super.pop();
      } else {
        break;
      }
    }    
    return link;
  }

  protected requeueReleased(): void {
    const stillHeld: ILink[] = [];
    for (const link of this.held) {
      if (this.filter.isHeld(link.url)) {
        stillHeld.push(link);
      } else {
        this.push(link);
      }
    }
    this.held = stillHeld;
  }
}