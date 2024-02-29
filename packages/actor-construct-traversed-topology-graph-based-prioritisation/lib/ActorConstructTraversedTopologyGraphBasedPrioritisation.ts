import type { IActionConstructTraversedTopology, IActorConstructTraversedTopologyOutput, IActorConstructTraversedTopologyArgs } from '@comunica/bus-construct-traversed-topology';
import { ActorConstructTraversedTopology } from '@comunica/bus-construct-traversed-topology';
import type { IActorTest } from '@comunica/core';
import { EdgeListGraph } from './EdgeListGraph';

import * as fs from 'fs'

/**
 * A comunica Url To Graph Construct Traversed Topology Actor.
 */
export class ActorConstructTraversedTopologyUrlToGraph extends ActorConstructTraversedTopology {
  public traversedGraph: EdgeListGraph;
  public actionsQueue: IActionConstructTraversedTopology[];

  public constructor(args: IActorConstructTraversedTopologyArgs) {
    super(args);
    this.traversedGraph = new EdgeListGraph();
    this.actionsQueue = [];
  }

  public async test(action: IActionConstructTraversedTopology): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionConstructTraversedTopology): Promise<IActorConstructTraversedTopologyOutput> {
    if (action.setDereferenced == true){
      for (let i = 0; i < action.links.length; i++) {
        const metaData = this.traversedGraph.getMetaDataNode(action.links[i].url);
        metaData.dereferenced = true;
        if (action.metadata[i].weight){
          metaData.weight = action.metadata[i].weight;
        }

        this.traversedGraph.updateMetaDataToDereferenced(action.links[i].url, metaData);
      }

      return {topology: this.traversedGraph}
    }

    for (let i = 0; i < action.links.length; i++) {
      this.traversedGraph.addEdge(this.getStrippedURL(action.links[i].url), this.getStrippedURL(action.parentUrl), action.metadata[i]);
    }
    
    return {topology: this.traversedGraph}
  }

  public getStrippedURL(url: string){
      return url.split('#')[0];
  }  
}
