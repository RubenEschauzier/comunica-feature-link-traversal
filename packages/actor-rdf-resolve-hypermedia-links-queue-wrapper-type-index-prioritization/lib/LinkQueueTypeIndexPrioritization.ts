import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActorExtractLinksSolidTypeIndex } from '../../actor-extract-links-solid-type-index/lib';

/**
 * A link queue that only allows the given number of links to be pushed into it.
 */
export class LinkQueueTypeIndexPrioritization extends LinkQueueWrapper {

  public constructor(linkQueue: ILinkQueue) {
    super(linkQueue);
  }

  public override push(link: ILink, parent: ILink): boolean {
    if (!link.metadata){
      link.metadata = { priority: 0 };
    }
    // Always prioritize links dereferenced from TypeIndex
    else if (link.metadata.producedByActor.name === "urn:comunica:default:extract-links/actors#solid-type-index"){
      link.metadata.priority = 2;
    }
    // Then prioritize new cards of Solidpods, which contain URIs to TypeIndexes
    else if (this.isSolidCard(link.url)){
      link.metadata.priority = 1;
    }
    else{
      link.metadata.priority = 0
    }
    console.log(`Priority: ${link.metadata?.priority}`);
    return super.push(link, parent);
  }

  private isSolidCard(url: string) {
    const normalizedUrl = url.split("#")[0];
    return normalizedUrl.endsWith("/profile/card");
  }
}
