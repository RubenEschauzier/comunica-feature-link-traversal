import { IActionConstructTraversedTopology, MediatorConstructTraversedTopology, Topology } from '@comunica/bus-construct-traversed-topology';
import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActionContext } from '@comunica/core';
import { ILinkPriority, LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { AdjacencyListGraphRcc } from '@comunica/actor-construct-traversed-topology-graph-based-prioritisation';

/**
 * A link queue that changes priorities based on indegree of nodes.
 */
export class LinkQueueRel1Prioritisation extends LinkQueueWrapper<LinkQueuePriority> {
  // The object that tracks topology during query execution, this needs to be rcc specific as it keeps track of data 
  // to make more efficient calculations
  trackedTopologyDuringQuery: AdjacencyListGraphRcc;

  public constructor(linkQueue: LinkQueuePriority, trackedTopology: AdjacencyListGraphRcc) {
    super(linkQueue);
    this.trackedTopologyDuringQuery = trackedTopology;
  }

  public push(link: ILink, parent: ILink): boolean {
    // Calculate priority of new node by taking the 1 step in-neighbourhood rcc scores
    const priorityLink = <ILinkPriority> link;

    const metadata = this.trackedTopologyDuringQuery.getMetaData(parent.url);
    if (!metadata){
      console.error(`Pushing link with parent that has unknown metadata: link: ${link.url}, parent link: ${parent.url}`);
      throw new Error('Invalid parent url');
    }
    priorityLink.priority = metadata['rcc'] > 0 ? 1 : 0;
    
    
    return super.push(priorityLink, parent);
  }

  public pop(): ILink | undefined {
    if (!this.linkQueue.isEmpty() && this.trackedTopologyDuringQuery.getChangedRccNodes().length > 0){
        // Rcc changed for some nodes, so priorities will need to be recalculated
        const rccNodeChanges = this.trackedTopologyDuringQuery.getChangedRccNodes();
        const nodeToIndex = this.trackedTopologyDuringQuery.getNodeToIndex();
        const indexToNode = this.trackedTopologyDuringQuery.getIndexToNode();
        const priorityQueueUrlToLink = this.linkQueue.urlToLink;
        
        for (const node in rccNodeChanges){
            const rccDiff = rccNodeChanges[node];
            const currentRcc = this.trackedTopologyDuringQuery.getMetaData(node)!['rcc'];
            // If rcc > 0 for the first time
            if (currentRcc - rccDiff == 0){
              console.log(`Rcc increased to ${currentRcc} with diff ${rccDiff}`)
              const indexNode = nodeToIndex[node];
              const outgoingEdges = this.trackedTopologyDuringQuery.getGraphDataStructure()[0];
              const outgoingEdgesNode = outgoingEdges[indexNode];
  
              for (const indegreeNeighbour of outgoingEdgesNode){
                  const neighbourUrl = indexToNode[indegreeNeighbour];
                  // Shoddy workaround because priority queue implementation is lacking. 
                  // Should make a increasePriority and increasePriorityRaw function. Problem is current increasePriority
                  // requires the index of the link in the queue, and updatePriority requires overriding the value of priority,
                  // it cna't increase it
                  const linkInQueue = priorityQueueUrlToLink[neighbourUrl];
                  if (linkInQueue){
                      console.log("Actually increased a priority")
                      // Increase the priority of child nodes by one to reflect new node with rcc >= 1
                      this.linkQueue.updatePriority(neighbourUrl, linkInQueue.priority + 1)
                  }
              }  
            }
        }

        // After calculation we have updated priorities so we can empty the list of unaccounted for changes
        this.trackedTopologyDuringQuery.resetChangedRccNodes()
    }

    const link = super.pop();

    return link;
  }
}
