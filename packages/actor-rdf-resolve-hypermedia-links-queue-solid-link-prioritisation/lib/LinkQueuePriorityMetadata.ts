import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import * as fs from 'fs'

export class LinkQueuePriorityMetadata implements ILinkQueue {
  public readonly links: ILink[];
  public readonly priorities: number[];

  public priorityDict: Record<string, number>;
  public numPriorities: number;
  public randomId: number;

  public constructor(possibleLinkSources: string[]) {
    this.priorities = [];
    this.links = [];

    this.priorityDict = {};
    possibleLinkSources.map((x, i) => this.priorityDict[x] = i); 
    this.numPriorities = possibleLinkSources.length
  }

  // Code for making figure nothing more should not be in release
  public appendToList(currentList: number[][], newList: number[]){
    return [...currentList, newList]
  }
  public appendToFile(newList: number[]){
    const logFileLoc = '/home/reschauz/projects/experiments-comunica/comunica-experiment-link-prioritisation/testNumDifferentPriorities/linkQueueEvolution.txt'
    const oldList: number[][] = JSON.parse(fs.readFileSync(logFileLoc, 'utf-8'));
    const toSaveList = this.appendToList(oldList, newList);
    fs.writeFileSync(logFileLoc, JSON.stringify(toSaveList));
  }
  // End code

  public pushPriority(link: ILink): boolean {
    // Insert link into queue, here we assume that we keep priorities sorted by always inserting at proper index
    const linkPriority = (!link.metadata?.source || !this.priorityDict[link.metadata.source]) ? this.numPriorities : this.priorityDict[link.metadata.source];
    const insertIndex = this.findInsertIndex(this.priorities, linkPriority);

    this.links.splice(insertIndex, 0, link);
    this.priorities.splice(insertIndex, 0, linkPriority);

    // Experiment code should never make it to final version
    this.appendToFile(this.priorities);
    // End experiment code

    return true;
  }

  public pushNonPriority(link: ILink): boolean{
    const linkPriority = (!link.metadata?.source || !this.priorityDict[link.metadata.source]) ? this.numPriorities : this.priorityDict[link.metadata.source];

    this.priorities.push(linkPriority);
    this.links.push(link);

    // Experiment code should never make it to final version
    this.appendToFile(this.priorities);
    // End experiment code

    return true
  }

  public push(link: ILink): boolean {
    return this.pushNonPriority(link);
  }

  public getSize(): number {
    return this.links.length;
  }

  public isEmpty(): boolean {
    return this.links.length === 0;
  }

  public pop(): ILink | undefined {
    this.priorities.shift();

    // Experiment code should not be in release
    this.appendToFile(this.priorities);
    // End code

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