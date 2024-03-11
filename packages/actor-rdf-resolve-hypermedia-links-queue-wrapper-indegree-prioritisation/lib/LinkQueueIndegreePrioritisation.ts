import { IActionConstructTraversedTopology, MediatorConstructTraversedTopology, Topology } from '@comunica/bus-construct-traversed-topology';
import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActionContext } from '@comunica/core';
import { ILinkPriority, LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';

/**
 * A link queue that changes priorities based on indegree of nodes.
 */
export class LinkQueueIndegreePrioritisation extends LinkQueueWrapper<LinkQueuePriority> {
  // Mapping from node id to indegree, used to find what priorities should change. Only contains nodes that have indegree > 1
  previousIndegrees: Record<number, number>;
  // Tracker that shows if priorities for entries in the queue might have changed
  numNodesMultipleParentPreviousPop: number;
  // The object that tracks topology during query execution
  trackedTopologyDuringQuery: Topology;

  public constructor(linkQueue: LinkQueuePriority, trackedTopology: Topology) {
    super(linkQueue);
    this.numNodesMultipleParentPreviousPop = 0;
    this.trackedTopologyDuringQuery = trackedTopology;
    this.previousIndegrees = {};
  }

  public push(link: ILink, parent: ILink): boolean {
    // Cast link to priority link and set default priority (= 0). Default priority is because indegree of new node is by definition 1
    const priorityLink = <ILinkPriority> link;
    priorityLink.priority = 0;
    
    return super.push(link, parent);
  }

  public pop(): ILink | undefined {
    // Disgusting implementation I don't like any part of this.
    // Before we pop, the link queue should determine whether priorities might have changed.
    // How do we efficiently store which indegrees should be recalculated???
    // Maybe we only recalculate the indegrees > 0, as most URLs will have only 1 parent (from the visualization stuff).

    // Map with nodes with indegree > 1 and in queue
    // Topology also has map with indegree > 1
    // This means the indegree
    if (this.trackedTopologyDuringQuery.getNumEdges() !== this.numNodesMultipleParentPreviousPop){
      // Hardcoded and specific to implementation, not super happy with it ...
      const incomingEdges = this.trackedTopologyDuringQuery.getGraphDataStructure()[1];
      const indexToNode = this.trackedTopologyDuringQuery.getIndexToNode();
      const newIndegrees: Record<number, number> = {};

      // Get indegrees, very inefficient I guess...
      for (let i = 0; i < incomingEdges.length; i++){
        if (incomingEdges[i].length > 1){
          newIndegrees[i] = incomingEdges[i].length;
        }
      }

      for (const nodeId in newIndegrees){
        if (!(nodeId in this.previousIndegrees) || this.previousIndegrees[nodeId] !== newIndegrees[nodeId] ){
          this.linkQueue.updatePriority(indexToNode[nodeId], newIndegrees[nodeId]);
        }
      }

      this.numNodesMultipleParentPreviousPop = this.trackedTopologyDuringQuery.getNumEdges();
      this.previousIndegrees = newIndegrees;
    }

    const link = super.pop();

    return link;
  }
}
