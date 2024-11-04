import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { ITopologyUpdate, StatisticTraversalTopology } from '../../statistic-traversal-topology/lib';

/**
 * A link queue that changes priorities based on indegree of nodes.
 */
export class LinkQueueIndegreePrioritisation extends LinkQueueWrapper<LinkQueuePriority>{
  public topologyStatistic: StatisticTraversalTopology;

  public adjacencyListIn: Record<number, number[]> = {};
  public indexToNode: Record<number, string> = {};
  public openNodes: number[] = [];
  public updated = false;

  public constructor(linkQueue: LinkQueuePriority, topologyStatistic: StatisticTraversalTopology) {
    super(linkQueue);
    topologyStatistic.on((data: ITopologyUpdate) => this.processTopologyUpdate(data));
  }

  public override push(link: ILink, parent: ILink): boolean {
    link.metadata = {...link.metadata, priority: 0 };
    
    return super.push(link, parent);
  }

  public override pop(): ILink | undefined {
    if (!super.isEmpty() && this.updated){
      this.updateIndegrees();
    }

    const link = super.pop();

    return link;
  }
  public override peek(): ILink | undefined {
    if (!super.isEmpty() && this.updated){
      this.updateIndegrees();
    }
    return super.peek();
  }

  public updateIndegrees(){
    const newIndegrees: Record<number, number> = {};

    // Get indegrees of still open nodes
    for (let i = 0; i < this.openNodes.length; i++){
      if (this.adjacencyListIn[this.openNodes[i]].length > 1){
        newIndegrees[i] = this.adjacencyListIn[this.openNodes[i]].length;
      }
    }

    // Iterate over keys
    for (const nodeId in newIndegrees){
        this.linkQueue.setPriority(this.indexToNode[nodeId], newIndegrees[nodeId]);
    }

    this.updated = false;
  }

  public processTopologyUpdate(data: ITopologyUpdate){
    this.adjacencyListIn = data.adjacencyListIn;
    this.indexToNode = data.indexToNodeDict;
    this.openNodes = data.openNodes;
    this.updated = true
  }
}
