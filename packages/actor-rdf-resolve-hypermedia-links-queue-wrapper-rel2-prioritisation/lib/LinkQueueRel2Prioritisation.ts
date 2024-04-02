import { IActionConstructTraversedTopology, MediatorConstructTraversedTopology, Topology } from '@comunica/bus-construct-traversed-topology';
import type { ILinkQueue, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActionContext } from '@comunica/core';
import { ILinkPriority, LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { AdjacencyListGraphRcc } from '@comunica/actor-construct-traversed-topology-graph-based-prioritisation';

/**
 * A link queue that changes priorities based on indegree of nodes.
 */
export class LinkQueueRel2Prioritisation extends LinkQueueWrapper<LinkQueuePriority> {
  // The object that tracks topology during query execution, this needs to be rcc specific as it keeps track of data 
  // to make more efficient calculations
  trackedTopologyDuringQuery: AdjacencyListGraphRcc;

  public constructor(linkQueue: LinkQueuePriority, trackedTopology: AdjacencyListGraphRcc) {
    super(linkQueue);
    this.trackedTopologyDuringQuery = trackedTopology;
  }

  public push(link: ILink, parent: ILink): boolean {
    const priorityLink = <ILinkPriority> link;
    priorityLink.priority = 0;
    const nodeToIndex = this.trackedTopologyDuringQuery.getNodeToIndex();

    if (nodeToIndex[link.url]){
      this.trackedTopologyDuringQuery.addPriorityToRecompute(nodeToIndex[link.url]);
    }
    
    return super.push(priorityLink, parent);
  }

  public pop(): ILink | undefined {
    const toRecompute = this.trackedTopologyDuringQuery.getPrioritiesToRecomputeSecondDegree();
    if (!super.isEmpty() && toRecompute.size > 0){
      this.updatePrioritiesRel2()
    }
    const link = super.pop();
    return link;
  }

  public peek(){
    const toRecompute = this.trackedTopologyDuringQuery.getPrioritiesToRecomputeSecondDegree();
    if (!super.isEmpty() && toRecompute.size > 0){
      this.updatePrioritiesRel2()
    }
    return super.peek();
  }


  public updatePrioritiesRel2(){
    const toRecompute = this.trackedTopologyDuringQuery.getPrioritiesToRecomputeSecondDegree();
    const incomingEdges = this.trackedTopologyDuringQuery.getGraphDataStructure()[1];
    const indexToNode = this.trackedTopologyDuringQuery.getIndexToNode();

    for (const node of toRecompute){
      const nodesVisited: Set<number> = new Set();
      const incomingNodes = incomingEdges[node];
      if (incomingNodes.length == 0){
        continue;
      }

      let newPriority = 0;
      for (const neighbourNode of incomingNodes){
        if (!nodesVisited.has(neighbourNode)){
          const neighbourMetadata = this.trackedTopologyDuringQuery.getMetaData(indexToNode[neighbourNode])!;
          if (neighbourMetadata['rcc'] > 0){
            newPriority += 1;
          }
          nodesVisited.add(neighbourNode);
        }
        for (const secondDegreeNeighbourNode of incomingEdges[neighbourNode]){
          if (!nodesVisited.has(secondDegreeNeighbourNode)){
            const secondDegreeNeighbourMetadata = this.trackedTopologyDuringQuery.getMetaData(indexToNode[secondDegreeNeighbourNode])!
            if (secondDegreeNeighbourMetadata['rcc'] > 0){
              newPriority += 1;
            }
            nodesVisited.add(secondDegreeNeighbourNode);  
          }
        }
      }
      this.linkQueue.updatePriority(indexToNode[node], newPriority);
    }
    this.trackedTopologyDuringQuery.resetPrioritiesToRecompute();
  }
}