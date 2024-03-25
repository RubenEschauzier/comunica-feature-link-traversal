

import { Topology } from "@comunica/bus-construct-traversed-topology";
/**
 * Data structure that denotes the traversed graph during link traversal. The graph stores an adjacency matrix,
 * the mapping between node index and url, and any metadata added for each node.
 */
export class TraversedGraph implements Topology{
  // String to 0 indexed index of a node, used to retrieve metadata
  private nodeToIndex: Record<string, number>;
  // Edges denoted by [start, end, weight] with all weights equal
  private edgeListUnWeighted: number[][];
  // Edges denoted by [start, end, weight] with weight equal to http request time
  private edgeListRequestTime: number[][];
  // Edges denoted by [start, end, weight] with weight equal to #quads in end node
  private edgeListDocumentSize: number[][];
  // Dictionary with string representations of edges, to check for duplicates
  private edgesInGraph: Record<string, number>;
  // 0 indexed list of all metadata associated with node, same order as nodeToIndex
  private metadataNode: Record<string, any>[];
  // Order in which the engine _dereferences_ nodes
  private traversalOrder: string[];
  private traversalOrderEdges: number[][];

  public constructor() {
    this.nodeToIndex = {};

    this.edgeListUnWeighted = [];
    this.edgeListRequestTime = [];
    this.edgeListDocumentSize = [];

    this.edgesInGraph = {};
    this.metadataNode = [];
    this.traversalOrder = [];
    this.traversalOrderEdges = [];  
  }

  public set(node: string, parent: string, metadata: Record<string, any>){
    // Self references edges are irrelevant
    if (node === parent){
      return true;
    }
    // Check if node is seedURL
    metadata.hasParent = true;
    if (this.nodeToIndex[parent] == undefined && !metadata.sourceNode) {
      console.warn('Adding node to traversed graph that has an unknown parent node')
      metadata.hasParent = false;
      // This only happens when we have a seed URL, so we add it to traversal order. As we don't
      // update metadata to dereferenced = true for seed URLs.
      if (metadata.dereferenced){
        this.traversalOrder.push(node)
      }
    }

    // Unseen nodes get registered
    if (!(node in this.nodeToIndex)){
      this.nodeToIndex[node] = Object.keys(this.nodeToIndex).length;
      this.metadataNode.push(metadata);
    }
    // If we have no parent, then there is also no edge to add
    if (metadata.hasParent === false){
      return true;
    }

    const edgeWeight: number = metadata.defaultEdgeWeight ? metadata.defaultEdgeWeight : 1;
    // Filter duplicate entries
    if (!(`${this.nodeToIndex[parent]}${this.nodeToIndex[node]}${edgeWeight}` in this.edgesInGraph)){
      // directed edge
      this.edgeListUnWeighted.push([this.nodeToIndex[parent], this.nodeToIndex[node], edgeWeight]);
      this.edgeListRequestTime.push([this.nodeToIndex[parent], this.nodeToIndex[node], edgeWeight]);
      this.edgeListDocumentSize.push([this.nodeToIndex[parent], this.nodeToIndex[node], edgeWeight]);

      this.edgesInGraph[`${this.nodeToIndex[parent]}${this.nodeToIndex[node]}${edgeWeight}`] = 1;
    }
    return true;
  }
  /**
   * Function that should only be called to update metadata of node to reflect 
   * that the URL corresponding to the node has been traversed
   * @param node 
   * @param metadata 
   */
  public setMetaDataDereferenced(node: string, metadata: Record<string, any>){
    // Optionally we can attach weight to dereferenced nodes. These weights are either HTTP request time, or document size
    // Of node. The weights are for edges pointing TO the dereferenced node.
    const updatedNodeIndex = this.nodeToIndex[node];

    if (metadata.weightHTTP){
      for (const edge of this.edgeListRequestTime){
        if (edge[1] === updatedNodeIndex){
          edge[2] = metadata.weightHTTP;
        }
      }
    }

    if (metadata.weightDocumentSize){
      for (const edge of this.edgeListDocumentSize){
        if (edge[1] === updatedNodeIndex){
          edge[2] = metadata.weightDocumentSize;
        }
      }
    }


    // We update traversal order when dereference event happens, and node not yet in traversalOrder
    // Note that no node will be dereferenced twice, so we don't need to check for existence of specific edges
    if (!this.traversalOrder.includes(node)){
      this.traversalOrder.push(node);
      // When we dereference a node, the node is always discovered using data from the first edge in
      // the topology, as all other possible parents are purely theoretical ways you can find the node
      let parent = -1;
      for (const edge of this.edgeListUnWeighted){
        if (edge[1] == updatedNodeIndex){
          parent = edge[0];
        }
      }
      this.traversalOrderEdges.push([parent, updatedNodeIndex]);
    }
    this.metadataNode[this.nodeToIndex[node]] = metadata;
  }

  public setMetaData(node: string, metadata: Record<string, any>) {
    this.metadataNode[this.nodeToIndex[node]] = metadata;
  }

  public getMetaData(node: string): Record<string, any> | undefined {
    return this.metadataNode[this.nodeToIndex[node]];
  }

  public getMetaDataAll(){
    return this.metadataNode;
  }
  
  public getNodeToIndex() {
    return this.nodeToIndex;
  }

  public getEdgeList(){
    return this.edgeListUnWeighted;
  }

  public getEdgeListHTTP(){
    return this.edgeListRequestTime;
  }

  public getEdgeListDocumentSize(){
    return this.edgeListDocumentSize;
  }

  public getTraversalOrderEdges(){
    return this.traversalOrderEdges;
  }

  public getGraphDataStructure(){
    throw new Error("Not implemented");
  }
  public resetTopology(){
    this.nodeToIndex = {};

    this.edgeListUnWeighted = [];
    this.edgeListRequestTime = [];
    this.edgeListDocumentSize = [];

    this.edgesInGraph = {};
    this.metadataNode = [];
    this.traversalOrder = [];
    this.traversalOrderEdges = [];
  }
}

