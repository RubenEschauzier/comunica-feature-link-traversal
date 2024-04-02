import { Topology } from "@comunica/bus-construct-traversed-topology";

/**
 * Data structure that denotes the traversed graph during link traversal. It allows the engine to track the result contribution 
 * score (rcc) for each node. This can be used for prioritisation
 */
export class AdjacencyListGraphRcc implements Topology {
  // Adjacencylist with each list at index $i$ representing the incoming edges of i, used to neighbourhood rcc score
  private readonly adjacencyListIncoming: number[][];
  // Adjacencylist with each list at index $i$ representing the outgoing edges of i, used to determine what nodes score
  // should be changed on change of rcc of node $i$
  private readonly adjacencyListOutgoing: number[][];

  // 0 indexed list of all metadata associated with node, same order as nodeToIndex
  private readonly metadataNode: Record<string, any>[];
  // String to 0 indexed index of a node, used to retrieve metadata
  private nodeToIndex: Record<string, number>;
  private indexToNode: Record<number, string>;
  // Nodes with changed rccs, with node string and increase in rcc
  private prioritiesToRecomputeFirstDegree: Set<number>;
  private prioritiesToRecomputeSecondDegree: Set<number>;

  public constructor() {
    this.nodeToIndex = {};
    this.indexToNode = {};

    this.metadataNode = [];
    this.adjacencyListIncoming = [];
    this.adjacencyListOutgoing = [];

    this.prioritiesToRecomputeFirstDegree = new Set();
    this.prioritiesToRecomputeSecondDegree = new Set();
  }

  public set(node: string, parent: string, metadata: Record<string, any>){
      // Self references edges are irrelevant
      if (node === parent){
        return true;
      }

      // Set Rcc counter to 0, this will only have effect on new nodes
      metadata['rcc'] = 0

      // Check if node is seedURL
      metadata.hasParent = true;
      if (this.nodeToIndex[parent] == undefined && !metadata.sourceNode) {
        console.warn('Adding node to traversed graph that has an unknown parent node')
        metadata.hasParent = false;
      }
      // If edge already exists do not add to graph
      if (this.nodeToIndex[node] && this.adjacencyListIncoming[this.nodeToIndex[node]].includes(this.nodeToIndex[parent])){
        return;
      }
  
      // If target node already exists and parent node not yet in incoming then we add incoming edge to target node
      if (this.nodeToIndex[node] && !this.adjacencyListIncoming[this.nodeToIndex[node]].includes(this.nodeToIndex[parent])){
        this.adjacencyListIncoming[this.nodeToIndex[node]].push(this.nodeToIndex[parent]);
      }
  
      // Unseen nodes get registered and entry into incoming edge added
      if (!(node in this.nodeToIndex)){
        this.nodeToIndex[node] = Object.keys(this.nodeToIndex).length;
        this.indexToNode[Object.keys(this.nodeToIndex).length-1] = node;
        this.metadataNode.push(metadata);
        // If not in node index we add new entries for adjacency list
        this.adjacencyListOutgoing.push([]);
        if (!(this.nodeToIndex[parent] == undefined)){
          this.adjacencyListIncoming.push([this.nodeToIndex[parent]]);
        }
        else{
          this.adjacencyListIncoming.push([]);
        }
      }
      
      // Add outgoing edge to parent node if it isn't already in the list
      if (this.adjacencyListOutgoing[this.nodeToIndex[parent]] && 
        !this.adjacencyListOutgoing[this.nodeToIndex[parent]].includes(this.nodeToIndex[node]))
      {
        this.adjacencyListOutgoing[this.nodeToIndex[parent]].push(this.nodeToIndex[node]);
      }

      // New node / edge means target node priority should be recomputed
      this.prioritiesToRecomputeFirstDegree.add(this.nodeToIndex[node]);
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

  public increaseRcc(node: string, increase: number){
    const nodeMetaData = this.metadataNode[this.nodeToIndex[node]];
    if (!nodeMetaData){
      throw new Error('Tried to increase rcc of node not in topology');
    }
    nodeMetaData.rcc += increase;
    // Determine neighbours that need to recompute their priorities due to change
    for (const neighbourNode of this.adjacencyListOutgoing[this.nodeToIndex[node]]){
      this.prioritiesToRecomputeFirstDegree.add(neighbourNode);
      this.prioritiesToRecomputeSecondDegree.add(neighbourNode);

      for (const secondDegreeNeighbour of this.adjacencyListOutgoing[neighbourNode]){
        this.prioritiesToRecomputeSecondDegree.add(secondDegreeNeighbour)
      }
    }
  }
  public addPriorityToRecompute(node: number){
    this.prioritiesToRecomputeFirstDegree.add(node);
    this.prioritiesToRecomputeSecondDegree.add(node);
  }

  public resetPrioritiesToRecompute(){
    this.prioritiesToRecomputeFirstDegree = new Set();
    this.prioritiesToRecomputeSecondDegree = new Set();
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

  public getIndexToNode(){
    return this.indexToNode;
  }
  /**
   * ABUSE OF INTERFACE!!!! How should I do this?
   * @returns The number of nodes that have multiple parents
   */
  public getNumEdges() {
    return this.nodeToIndex.length;
  }

  public getGraphDataStructure(){
    return  [this.adjacencyListOutgoing, this.adjacencyListIncoming];
  }

  public getPrioritiesToRecomputeFirstDegree(){
    return this.prioritiesToRecomputeFirstDegree;
  }
  
  public getPrioritiesToRecomputeSecondDegree(){
    return this.prioritiesToRecomputeSecondDegree;
  }
}

