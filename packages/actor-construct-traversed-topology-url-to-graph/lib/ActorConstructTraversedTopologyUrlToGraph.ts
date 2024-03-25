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

  public async run(action: IActionConstructTraversedTopology): Promise<IActorConstructTraversedTopologyOutput> {
    if (action.setDereferenced == true){
      for (let i = 0; i < action.links.length; i++) {
        const URI = decodeURI(this.getStrippedEncodedURL(action.links[i].url));
        const metaData = this.traversedGraph.getMetaData(URI)!;
        metaData.parent = decodeURI(metaData.parent);

        metaData.dereferenced = true;
        if (action.metadata[i].weightHTTP){
          metaData.weightHTTP = action.metadata[i].weightHTTP;
        }
        if (action.metadata[i].weightDocumentSize){
          metaData.weightDocumentSize = action.metadata[i].weightDocumentSize;
        }

        this.traversedGraph.setMetaDataDereferenced(URI, metaData);
      }

      return {topology: this.traversedGraph}
    }

    for (let i = 0; i < action.links.length; i++) {
      const URI = decodeURI(this.getStrippedEncodedURL(action.links[i].url));
      const parentURI = decodeURI(this.getStrippedEncodedURL(action.parentUrl))
      this.traversedGraph.set(URI, parentURI, action.metadata[i]);
    }
    
    return {topology: this.traversedGraph}
  }

  public getStrippedEncodedURL(url: string){
      return url.split('#')[0];
  }  
}
