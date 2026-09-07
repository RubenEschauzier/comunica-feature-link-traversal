import type { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { ILink } from '@comunica/types';

/**
 * A link queue that prioritizes links found using a derived resource predicate extractor.
 */
export class LinkQueueDerivedResourcePrioritization extends LinkQueueWrapper<LinkQueuePriority> {
  /**
   * The priority given to derived resource links, so they are always dereferenced
   * before any other link in the queue.
   */
  public static readonly derivedResourcePriority = 1e9;

  protected readonly derivedResourcePredicate: string;

  public constructor(linkQueue: LinkQueuePriority, derivedResourcePredicate: string) {
    super(linkQueue);
    this.derivedResourcePredicate = derivedResourcePredicate;
  }

  public override push(link: ILink, parent?: ILink): boolean {
    // Links that are not derived resources keep whatever priority they were given,
    // the wrapped priority queue defaults it to 0 when it is absent.
    if (this.isDerivedResourceLink(link)) {
      link.metadata = {
        ...link.metadata,
        priority: LinkQueueDerivedResourcePrioritization.derivedResourcePriority,
      };
    }
    return super.push(link, parent);
  }

  public override pop(): ILink | undefined {
    return super.pop(); ;
  }

  public override peek() {
    return super.peek();
  }

  protected isDerivedResourceLink(link: ILink) {
    const producedBy: Record<string, any> | undefined = link.metadata ? link.metadata["producedByActor"] : undefined;
    if (producedBy){
      const matchingPredicate: string | undefined = producedBy["matchingPredicate"];
      if (matchingPredicate === this.derivedResourcePredicate){
        return true
      }
    }
    return false;
  }
}