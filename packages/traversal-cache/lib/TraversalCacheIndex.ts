/**
 * TODO: To build this index we need to know ALL predicates in a file that map to a given node. This cache index will infinitely grow
 * It needs a method to prune elements. This seems somewhat complicated, can be done by (hierarchical) contraction when node gets deleted
 * the child nodes of that node will be connected to parent nodes of the node (with additional information of required predicates). 
 * This information might lead to predicate sets that are required for a connection instead of a singular predicate.
 */

export class TraversalCacheIndex<V extends {}> implements ITraversalIndex<V>{
    /**
     * Data structures that maps a predicate string to a number
     */
    private predicateToIndex: Record<string, number> = {};
    private indexToPredicate: Record<number, string> = {};
    /**
     * Datastructure that maps a node to a number
     */
    private nodeToIndex: Record<string, number> = {};
    private indexToNode: Record<number, string> = {};
    /**
     * Datastructure mapping an edge string representation (JSON.stringify([start, end])) to
     * all predicate (indexes) that enable this connection
     */
    private edgeToPredicates: Record<number, Set<number>> = {};
    /**
     * Data structure storing the connections of the graph.
     */
    private edgeDict: Record<number, number[]> = {};

    public constructor(){

    }

    public traverse(start: string, reachableEdges: string[]): V[] {
        throw new Error("Method not implemented.");
    }

    public clear(){
        throw new Error("Method not implemented.");
    }
}

export interface ITraversalIndex<V extends {}>{
    /**
     * Traverses the index to find all reachable nodes given a start node
     * and acceptable edge types.
     * @param start 
     * @param reachableEdges 
     */
    traverse(start: string, reachableEdges: string[]): V[]
    /**
     * Empties the traversal index
     */
    clear(): void;
}