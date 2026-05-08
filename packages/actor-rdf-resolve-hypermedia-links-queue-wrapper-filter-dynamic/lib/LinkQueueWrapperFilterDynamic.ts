import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { ILinkQueue, ILink } from '@comunica/types';
import { IDynamicFilter } from '@comunica/types-link-traversal';
import { minimatch } from 'minimatch'
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
      if (this.matchesFilter(link.url)) {
        link = super.pop();
      } else {
        break;
      }
    }    
    return link;
  }

  /**
   * Evaluates the URL against the live filter object.
   */
  private matchesFilter(url: string): boolean {
    if (this.filter.exact.has(url)) {
      return true;
    }
    return this.filter.regExp.some((exp) => exp.test(url));
  }
}