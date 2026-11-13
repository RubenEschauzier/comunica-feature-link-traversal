import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { ILink, ILinkQueue } from '@comunica/types';

/**
 * A link queue that refuses links into the namespaces of known query sources.
 *
 * A known source, such as a SPARQL endpoint given with the query, is the only way its data is accessed:
 * the documents in its namespaces hold data the query already asks it for.
 */
export class LinkQueueWrapperKnownSources extends LinkQueueWrapper {
  public constructor(linkQueue: ILinkQueue, private readonly namespaces: readonly string[]) {
    super(linkQueue);
  }

  /**
   * Whether the url lies in a namespace a known source covers.
   */
  public isKnown(url: string): boolean {
    return this.namespaces.some(namespace => url.startsWith(namespace));
  }

  public override push(link: ILink, parent?: ILink): boolean {
    if (this.isKnown(link.url)) {
      return false;
    }
    return super.push(link, parent);
  }
}
