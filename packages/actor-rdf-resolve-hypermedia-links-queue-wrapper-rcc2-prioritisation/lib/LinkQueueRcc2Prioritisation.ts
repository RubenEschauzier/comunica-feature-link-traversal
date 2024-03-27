import { IActionConstructTraversedTopology, MediatorConstructTraversedTopology, Topology } from '@comunica/bus-construct-traversed-topology';
import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActionContext } from '@comunica/core';
import { ILinkPriority, LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { AdjacencyListGraphRcc } from '@comunica/actor-construct-traversed-topology-graph-based-prioritisation';

/**
 * A link queue that changes priorities based on indegree of nodes.
 */
export class LinkQueueRcc2Prioritisation extends LinkQueueWrapper<LinkQueuePriority> {
  // The object that tracks topology during query execution, this needs to be rcc specific as it keeps track of data 
  // to make more efficient calculations
  trackedTopologyDuringQuery: AdjacencyListGraphRcc;

  public constructor(linkQueue: LinkQueuePriority, trackedTopology: AdjacencyListGraphRcc) {
    super(linkQueue);
    this.trackedTopologyDuringQuery = trackedTopology;
  }

  public push(link: ILink, parent: ILink): boolean {

    // Calculate priority of new node by summing the 2 step in-neighbourhood rcc scores
    const priorityLink = <ILinkPriority> link;
    const inEdges = this.trackedTopologyDuringQuery.getGraphDataStructure()[1];
    const nodeToIndex = this.trackedTopologyDuringQuery.getNodeToIndex();
    const indexToNode = this.trackedTopologyDuringQuery.getIndexToNode();
    const metadataParent = this.trackedTopologyDuringQuery.getMetaData(parent.url);

    let priority = 0;

    // Add parent rcc to new link
    if (!metadataParent){
      console.error(`Pushing link with parent that has unknown metadata: link: ${link.url}, parent link: ${parent.url}`);
      throw new Error('Invalid parent url');
    }
    priority += metadataParent['rcc'];

    // Add in neighbourhood of parent to to new link
    for (const parentOfParent of inEdges[nodeToIndex[parent.url]]){
      const metadataParentofParent = this.trackedTopologyDuringQuery.getMetaData(indexToNode[parentOfParent]);
      if (!metadataParentofParent){
        console.error(`Pushing link with parentOfParent that has unknown metadata: link: ${link.url}, parentOfParent link: ${indexToNode[parentOfParent]}`);
        throw new Error('Invalid parentOfParent url');
      }
      priority += metadataParentofParent['rcc'];
    }
    priorityLink.priority = priority;
    
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
                // Increase the priority of child nodes by the change in rcc score if node is yet to be dereferenced
                this.linkQueue.updatePriority(neighbourUrl, linkInQueue.priority + rccNodeChanges[node])
            }
            // Update second degree neighbours
            const outgoingEdgesNeighbour = outgoingEdges[indegreeNeighbour];
            for (const indegreeSecondDegreeNeighbour of outgoingEdgesNeighbour){
              const neighbourUrlSecondDegree = indexToNode[indegreeSecondDegreeNeighbour];
              const linkInQueue = priorityQueueUrlToLink[neighbourUrlSecondDegree];

              if (linkInQueue){
                  // Increase the priority of child nodes by the change in rcc score if node is yet to be dereferenced
                  this.linkQueue.updatePriority(neighbourUrlSecondDegree, linkInQueue.priority + rccNodeChanges[node])
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
