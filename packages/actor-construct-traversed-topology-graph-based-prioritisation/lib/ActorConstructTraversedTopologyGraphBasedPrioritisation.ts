import type { IActionConstructTraversedTopology, IActorConstructTraversedTopologyOutput, IActorConstructTraversedTopologyArgs } from '@comunica/bus-construct-traversed-topology';
import { ActorConstructTraversedTopology } from '@comunica/bus-construct-traversed-topology';
import type { IActorTest } from '@comunica/core';
import { AdjacencyListGraph } from './AdjacencyListGraph';

import * as fs from 'fs'

/**
 * A comunica Graph-based-Prioritisation Construct Traversed Topology Actor.
 */
export class ActorConstructTraversedTopologyUrlToGraph extends ActorConstructTraversedTopology {
  public traversedGraph: AdjacencyListGraph;

  public constructor(args: IActorConstructTraversedTopologyArgs) {
    super(args);
    this.traversedGraph = new AdjacencyListGraph();
  }

  public async test(action: IActionConstructTraversedTopology): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionConstructTraversedTopology): Promise<IActorConstructTraversedTopologyOutput> {
    if (action.setDereferenced == true){
      for (let i = 0; i < action.links.length; i++) {
        const metaData = this.traversedGraph.getMetaData(action.links[i].url)!;
        metaData.dereferenced = true;
        this.traversedGraph.setMetaData(action.links[i].url, metaData);
      }

      return {topology: this.traversedGraph}
    }

    for (let i = 0; i < action.links.length; i++) {
      this.traversedGraph.set(this.getStrippedURL(action.links[i].url), this.getStrippedURL(action.parentUrl), action.metadata[i]);
    }
    return {topology: this.traversedGraph}
  }

  public getStrippedURL(url: string){
      return url.split('#')[0];
  }  
}
