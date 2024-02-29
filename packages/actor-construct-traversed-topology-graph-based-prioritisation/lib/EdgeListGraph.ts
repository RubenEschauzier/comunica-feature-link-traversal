import { Topology } from "@comunica/bus-construct-traversed-topology";
// USE THIS FOR PAGERANK: https://www.npmjs.com/package/pagerank-js
/**
 * Data structure that denotes the traversed graph during link traversal. The graph stores an adjacency matrix,
 * the mapping between node index and url, and any metadata added for each node.
 */
export class AdjacencyListGraph implements Topology {
  // Adjacencylist with each list at index $i$ representing the outgoing edges of i
  private readonly adjacencyList: number[][];
  // 0 indexed list of all metadata associated with node, same order as nodeToIndex
  private readonly metadataNode: Record<string, any>[];
  // String to 0 indexed index of a node, used to retrieve metadata
  private nodeToIndex: Record<string, number>;

  public constructor() {
    this.nodeToIndex = {};
    this.metadataNode = [];
  }

  public set(node: string, parent: string, metadata: Record<string, any>){
    // TODO: When we call set, we should either calculate the metric here OR we should indicate that we changed the topology so
    // the priority queue knows it should recalculate some priorities. Second is likely WAY better!
    // TODO: Test implementation
    // Self references edges are irrelevant
    if (node === parent){
      return true;
    }

    // Check if node is seedURL
    metadata.hasParent = true;
    if (this.nodeToIndex[parent] == undefined && !metadata.sourceNode) {
      console.warn('Adding node to traversed graph that has an unknown parent node')
      metadata.hasParent = false;
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
    // Add the discovered node to adjacencylist with self reference.
    this.adjacencyList.push([this.nodeToIndex[node]]);

    // Add outgoing edge to parent node if it isn't already in the list
    if (!this.adjacencyList[this.nodeToIndex[parent]].includes(this.nodeToIndex[node])){
      this.adjacencyList[this.nodeToIndex[parent]].push(this.nodeToIndex[node]);
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
    // This doesn't do anything special when something is dereferenced
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

  public getGraphDataStructure(){
    throw new Error("Not implemented");
  }
}

