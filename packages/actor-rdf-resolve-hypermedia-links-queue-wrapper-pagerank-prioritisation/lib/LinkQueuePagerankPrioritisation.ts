import { Topology } from '@comunica/bus-construct-traversed-topology';
import type { ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ILinkPriority, LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority'
const pagerank = require('pagerank-js');

/**
 * A link queue that changes priorities based on indegree of nodes.
 */
export class LinkQueuePagerankPrioritisation extends LinkQueueWrapper<LinkQueuePriority> {
  // The object that tracks topology during query execution
  trackedTopologyDuringQuery: Topology;

  public constructor(linkQueue: LinkQueuePriority, trackedTopology: Topology) {
    super(linkQueue);
    this.trackedTopologyDuringQuery = trackedTopology;
  }

  public push(link: ILink, parent: ILink): boolean {
    // Cast link to priority link and set default priority (= 0). Default priority is because indegree of new node is by definition 1
    const priorityLink = <ILinkPriority> link;
    priorityLink.priority = 0;
    return super.push(link, parent);
  }

  public pop(): ILink | undefined {
    // Only recalculate whenever we actually pop a link
    if (super.getSize() > 0 ){
      this.calculatePrioritiesPagerank()
    }

    const link = super.pop();

    return link;
  }

  public peek(): ILink | undefined {
    if (super.getSize() > 0 ){
      this.calculatePrioritiesPagerank()
    }

    const link = super.peek();

    return link;

  }

  public calculatePrioritiesPagerank(){
    // pagerank here
    const edgeListOutgoing = this.trackedTopologyDuringQuery.getGraphDataStructure()[0];
    const indexToNode = this.trackedTopologyDuringQuery.getIndexToNode();

    try{
      const rankings = pagerank(edgeListOutgoing, 0.85, 0.001, function(err: any, res:any) {
          if (err) console.log(err)
      });
      const urlToPriority: Record<string, number> = {};

      for (let i = 0; i < rankings.probabilityNodes.length; i++){
          urlToPriority[indexToNode[i]] = rankings.probabilityNodes[i];
      }

      this.linkQueue.updateAllPriority(urlToPriority);
    }
    catch(err){
      console.log(err)
    }
  }
}
