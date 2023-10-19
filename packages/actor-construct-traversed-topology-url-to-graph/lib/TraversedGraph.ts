/**
 * Data structure that denotes the traversed graph during link traversal. The graph stores an adjacency matrix,
 * the mapping between node index and url, and any metadata added for each node.
 */
export class TraversedGraph {
  private nodeToIndex: Record<string, number>;
  private readonly adjacencyMatrix: number[][];
  private readonly metadataNode: Record<string, any>[];

  public constructor() {
    this.nodeToIndex = {};
    this.adjacencyMatrix = [];
    this.metadataNode = [];
  }

  public addNode(node: string, parent: string, metadata: Record<string, any>) {
    if (this.nodeToIndex[node]){
      console.log("We are adding node that already exists");
    }
    metadata.hasParent = true;
    if (this.nodeToIndex[parent] == undefined && !metadata.sourceNode) {
      console.warn('Adding node to traversed graph that has an unknown parent node')
      metadata.hasParent = false
      // throw new Error('Adding node to traversed graph that has an unknown parent node');
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
}

