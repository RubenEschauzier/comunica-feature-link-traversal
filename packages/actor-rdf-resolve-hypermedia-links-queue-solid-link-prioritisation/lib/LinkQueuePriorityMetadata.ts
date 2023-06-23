import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';


export class LinkQueuePriorityMetadata implements ILinkQueue {
  public readonly links: ILink[];
  public readonly priorities: number[];

  public priorityDict: Record<string, number>;
  public numPriorities: number

  public constructor(possibleLinkSources: string[]) {
    this.priorities = [];
    this.links = [];

    this.priorityDict = {};
    possibleLinkSources.map((x, i) => this.priorityDict[x] = i); 

    this.numPriorities = possibleLinkSources.length
  }


  public push(link: ILink): boolean {
    // Insert link into queue, here we assume that we keep priorities sorted by always inserting at proper index
    const linkPriority = (!link.metadata?.source || !this.priorityDict[link.metadata.source]) ? this.numPriorities : this.priorityDict[link.metadata.source];
    const insertIndex = this.findInsertIndex(this.priorities, linkPriority);

    this.links.splice(insertIndex, 0, link);
    this.priorities.splice(insertIndex, 0, linkPriority);
    return true;
  }

  public getSize(): number {
    return this.links.length;
  }

  public isEmpty(): boolean {
    return this.links.length === 0;
  }

  public pop(): ILink | undefined {
    this.priorities.shift();
    return this.links.shift();
  }

  public peek(): ILink | undefined {
    return this.links[0];
  }

  private findInsertIndex(priorityQueue: number[], priorityToInsert: number){
    let low = 0,
    high = priorityQueue.length;

    while (low < high) {
        let mid = (low + high) >>> 1;
        if (priorityQueue[mid] < priorityToInsert) low = mid + 1;
        else high = mid;
    }

    // Ensure FIFO property for links with equal priority
    while (low < priorityQueue.length){
      if (priorityQueue[low + 1] == priorityToInsert) low += 1;
      else break;
    }
    return low;
  }
}