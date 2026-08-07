import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { ILinkQueue, ILink } from '@comunica/types';
import { IDynamicFilter } from '@comunica/types-link-traversal';
import { minimatch } from 'minimatch';

/**
 * A link queue wrapper that dynamically filters links using a live object 
 * of exact matches and pre-compiled regular expressions.
 */
export class LinkQueueWrapperFilterDynamic extends LinkQueueWrapper {
  private readonly filter: IDynamicFilter;

  public constructor(linkQueue: ILinkQueue, filterDynamic: IDynamicFilter) {
    super(linkQueue);
    this.filter = filterDynamic;
  }

  public override pop(): ILink | undefined {
    let link = super.pop();
    while (link) {
      if (this.filter.matchesFilter(link.url)) {
        link = super.pop();
      } else {
        break;
      }
    }    
    return link;
  }
}