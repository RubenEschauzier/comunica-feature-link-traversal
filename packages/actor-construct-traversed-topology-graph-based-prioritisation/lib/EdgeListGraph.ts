/**
 * Data structure that denotes the traversed graph during link traversal. The graph stores an adjacency matrix,
 * the mapping between node index and url, and any metadata added for each node.
 */
export class EdgeListGraph {
  // String to 0 indexed index of a node, used to retrieve metadata
  private nodeToIndex: Record<string, number>;
  // Left-over from adj matrix implementation, can be removed in final version
  private readonly adjacencyMatrix: number[][];
  // Edges denoted by [start, end, weight]
  private readonly edgeList: number[][];
  // Dictionary with string representations of edges, to check for duplicates
  private readonly edgesInGraph: Record<string, number>;
  // 0 indexed list of all metadata associated with node, same order as nodeToIndex
  private readonly metadataNode: Record<string, any>[];
  // Order in which the engine _dereferences_ nodes
  private readonly traversalOrder: string[];

  public constructor() {
    this.nodeToIndex = {};
    this.adjacencyMatrix = [];
    this.metadataNode = [];
    this.traversalOrder = [];
    this.edgeList = [];
    this.edgesInGraph = {};
  }

  public addNode(node: string, parent: string, metadata: Record<string, any>) {
    metadata.hasParent = true;
    if (this.nodeToIndex[parent] == undefined && !metadata.sourceNode) {
      console.warn('Adding node to traversed graph that has an unknown parent node')
      metadata.hasParent = false
      // This only happens when we have a seed URL, so we add it to traversal order. As we don't
      // update metadata to dereferenced = true for seed URLs.
      if (metadata.dereferenced){
        this.traversalOrder.push(node)
      }
    }
    // If we find node already seen, we update the parent nodes if needed
    if (node in this.nodeToIndex){
      this.adjacencyMatrix[this.nodeToIndex[node]][this.nodeToIndex[parent]] = 1;
      return false;
    }
    // If we add first node we initialise the matrix
    if (this.adjacencyMatrix.length === 0) {
      this.adjacencyMatrix.push([ 1 ]);
    }

    // If matrix initialised we update it
    else {
      for (const matrixRow of this.adjacencyMatrix) {
        matrixRow.push(0);
      }
      const newRow = new Array<number>(this.adjacencyMatrix.length + 1).fill(0);
      // Add connection to parent node and self connection
      newRow[this.nodeToIndex[parent]] = 1;
      newRow[newRow.length - 1] = 1;
      this.adjacencyMatrix.push(newRow);
    }

    // Update node to Index
    this.nodeToIndex[node] = this.adjacencyMatrix.length - 1;

    // Add metadata to graph
    this.metadataNode.push(metadata);
    return true;
  }

  public addEdge(node: string, parent: string, metadata: Record<string, any>){
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

    const edgeWeight: number = metadata.weight ? metadata.weight : 1;
    // Filter duplicate entries
    if (!(`${this.nodeToIndex[parent]}${this.nodeToIndex[node]}${edgeWeight}` in this.edgesInGraph)){
      // directed edge
      this.edgeList.push([this.nodeToIndex[parent], this.nodeToIndex[node], edgeWeight]);
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
  public updateMetaDataToDereferenced(node: string, metadata: Record<string, any>){
    // If we dereference the node and want to attach weights to the node based on either dereferenced file size
    // or http request time we update all edges that end in the dereferenced node. So where edge[1] = node

    // TODO: Include some way to specify which weights you want to track in the edgelist, as we want to be able to track
    // both http request time and # Triples
    if (metadata.weight){
      const updatedNodeIndex = this.nodeToIndex[node];
      for (const edge of this.edgeList){
        if (edge[1] === updatedNodeIndex){
          edge[2] = metadata.weight;
        }
      }
    }
    // We update traversal order when dereference event happens
    this.traversalOrder.push(node);
    this.metadataNode[this.nodeToIndex[node]] = metadata;
  }

  public setMetaDataNode(node: string, metadata: Record<string, any>) {
    this.metadataNode[this.nodeToIndex[node]] = metadata;
  }

  public getMetaDataNode(node: string) {
    return this.metadataNode[this.nodeToIndex[node]];
  }

  public getAdjacencyMatrix() {
    return this.adjacencyMatrix;
  }

  public getMetaDataAll(){
    return this.metadataNode;
  }
  
  public getNodeToIndexes() {
    return this.nodeToIndex;
  }

  public getEdgeList(){
    return this.edgeList;
  }
}

