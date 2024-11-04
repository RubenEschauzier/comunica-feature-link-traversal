import type { ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority'
import { ITopologyUpdate, StatisticTraversalTopology } from '../../statistic-traversal-topology/lib';
const pagerank = require('pagerank-js');

/**
 * A link queue that changes priorities based on indegree of nodes.
 */
export class LinkQueuePagerankPrioritisation extends LinkQueueWrapper<LinkQueuePriority> {
  // The object that tracks topology during query execution
  public adjacencyListIn: Record<number, number[]>;
  public adjacencyListOut: Record<number, number[]>;
  public nodeToIndex: Record<string, number>;
  public indexToNode: Record<number, string>;
  public updated: boolean = true;

  public constructor(linkQueue: LinkQueuePriority, topologyStatistic: StatisticTraversalTopology) {
    super(linkQueue);
    // Set-up handler for topology updates
    topologyStatistic.on((data: ITopologyUpdate) => this.processTopologyUpdate(data));
    
    this.adjacencyListIn = {};
    this.adjacencyListOut = {};
    this.nodeToIndex = {};
    this.indexToNode = {};
  }

  public override push(link: ILink, parent: ILink): boolean {
    // Cast link to priority link and set default priority (= 0). Default priority is because indegree of new node is by definition 1
    link.metadata = {...link.metadata, priority: 0 };
    return super.push(link, parent);
  }

  public override pop(): ILink | undefined {
    // Only recalculate whenever we actually pop a link
    if (super.getSize() > 0 && this.updated === true){
      this.calculatePrioritiesPagerank()
    }

    const link = super.pop();
    this.updated = false;
    return link;
  }

  public override peek(): ILink | undefined {
    if (super.getSize() > 0 && this.updated === true){
      this.calculatePrioritiesPagerank()
    }

    const link = super.peek();
    this.updated = false;
    return link;

  }

  public calculatePrioritiesPagerank(){
    // pagerank here
    if (this.updated){
      // Convert to array
      const edgeListArray: number[][] = [];
      Object.keys(this.adjacencyListOut).forEach((key: string) => {
        const index: number = parseInt(key, 10);
        edgeListArray[index] = this.adjacencyListOut[index];
      });
      const rankings = pagerank(edgeListArray, 0.85, 0.001, function(err: any, res:any) {
        if (err) console.log(err)
      });

      const urlToPriority: Record<string, number> = {};
      for (let i = 0; i < rankings.probabilityNodes.length; i++){
          urlToPriority[this.indexToNode[i]] = rankings.probabilityNodes[i];
      }

      this.linkQueue.setAllPriority(urlToPriority);
    }
  }

  public processTopologyUpdate(data: ITopologyUpdate){
    this.adjacencyListIn = data.adjacencyListIn;
    this.adjacencyListOut = data.adjacencyListOut;
    this.indexToNode = data.indexToNodeDict;
    this.nodeToIndex = data.nodeToIndexDict;
    this.updated = true
  }
}
