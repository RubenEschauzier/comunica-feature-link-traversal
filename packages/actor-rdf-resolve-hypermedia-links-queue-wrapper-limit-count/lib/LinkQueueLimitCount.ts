import { IActionConstructTraversedTopology, MediatorConstructTraversedTopology } from '@comunica/bus-construct-traversed-topology';
import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActionContext } from '@comunica/core';

/**
 * A link queue that only allows the given number of links to be pushed into it.
 */
export class LinkQueueLimitCount extends LinkQueueWrapper {
  private limit: number;
  private readonly mediatorConstructTraversedTopology: MediatorConstructTraversedTopology;

  public constructor(linkQueue: ILinkQueue, limit: number, mediatorConstructTraversedTopology: MediatorConstructTraversedTopology) {
    super(linkQueue);
    this.limit = limit;
    this.mediatorConstructTraversedTopology = mediatorConstructTraversedTopology;
  }

  public push(link: ILink, parent: ILink): boolean {
    if (this.limit === 0) {
      return false;
    }
    this.limit--;
    return super.push(link, parent);
  }

  public pop(): ILink | undefined {
    const link = super.pop();
    if (link){
      // For this we have to add new actor that only runs when updateMetaData = true, and add check for other actors to only run if updateMetaData = false      
      const action: IActionConstructTraversedTopology = {
        parentUrl: '',
        links: [link],
        metadata: [{}],
        context: new ActionContext({}),
        setDereferenced: true
      }  
      this.mediatorConstructTraversedTopology.mediate(action);
    }
    return link;
  }
}
