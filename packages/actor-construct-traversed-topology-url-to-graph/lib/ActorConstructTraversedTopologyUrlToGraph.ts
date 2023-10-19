import type { IActionConstructTraversedTopology, IActorConstructTraversedTopologyOutput, IActorConstructTraversedTopologyArgs } from '@comunica/bus-construct-traversed-topology';
import { ActorConstructTraversedTopology } from '@comunica/bus-construct-traversed-topology';
import type { IActorTest } from '@comunica/core';
import { TraversedGraph } from './TraversedGraph';

import * as fs from 'fs'

/**
 * A comunica Url To Graph Construct Traversed Topology Actor.
 */
export class ActorConstructTraversedTopologyUrlToGraph extends ActorConstructTraversedTopology {
  public traversedGraph: TraversedGraph;
  public actionsQueue: IActionConstructTraversedTopology[];

  public constructor(args: IActorConstructTraversedTopologyArgs) {
    super(args);
    this.traversedGraph = new TraversedGraph();
    this.actionsQueue = [];
  }

  public async test(action: IActionConstructTraversedTopology): Promise<IActorTest> {
    return true;
  }

  public dequeueAction(){

  }

  public runContinuousAction(){

  }

  public async run(action: IActionConstructTraversedTopology): Promise<IActorConstructTraversedTopologyOutput> {
    if (action.setDereferenced == true){
      for (let i = 0; i < action.links.length; i++) {
        const metaData = this.traversedGraph.getMetaDataNode(action.links[i].url);
        metaData.dereferenced = true;
        this.traversedGraph.setMetaDataNode(action.links[i].url, metaData);
      }
      fs.writeFileSync('/home/reschauz/projects/experiments-comunica/comunica-experiment-performance-metric/metaDataTemp.txt', 
      JSON.stringify(this.traversedGraph.getMetaDataAll()));
  
      return true;
    }

    for (let i = 0; i < action.links.length; i++) {
      this.traversedGraph.addNode(action.links[i].url, action.parentUrl, action.metadata[i]);
    }
    // Temp way to get data from graph
    fs.writeFileSync('/home/reschauz/projects/experiments-comunica/comunica-experiment-performance-metric/adjMatrixTemp.txt', 
    JSON.stringify(this.traversedGraph.getAdjacencyMatrix()));
    fs.writeFileSync('/home/reschauz/projects/experiments-comunica/comunica-experiment-performance-metric/metaDataTemp.txt', 
    JSON.stringify(this.traversedGraph.getMetaDataAll()));
    console.log(this.traversedGraph.getMetaDataAll());
    console.log(JSON.stringify(this.traversedGraph.getMetaDataAll()));
    fs.writeFileSync('/home/reschauz/projects/experiments-comunica/comunica-experiment-performance-metric/nodeToIndex.txt', 
    JSON.stringify(this.traversedGraph.getNodeToIndexes()));
    
    return true;
  }
}
