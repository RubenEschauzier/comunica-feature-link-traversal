/**
 * TODO: To build this index we need to know ALL predicates in a file that map to a given node. This cache index will infinitely grow
 * It needs a method to prune elements. This seems somewhat complicated, can be done by (hierarchical) contraction when node gets deleted
 * the child nodes of that node will be connected to parent nodes of the node (with additional information of required predicates).
 * This information might lead to predicate sets that are required for a connection instead of a singular predicate.
 */

import type { IDiscoverEventData, IStatisticBase } from '@comunica/types';

export class TraversalCacheIndex<K extends string> implements ITraversalIndex<K> {
  /**
   * Data structures that maps a predicate string to a number
   */
  private readonly predicateToIndex: Record<string, number> = {};
  private readonly indexToPredicate: Record<number, string> = {};
  /**
   * Datastructure that maps a node to a number
   */
  private readonly nodeToIndex: Record<string, number> = {};
  private readonly indexToNode: Record<number, string> = {};
  /**
   * Datastructure mapping an edge string representation (JSON.stringify([start, end])) to
   * all predicate (indexes) that enable this connection
   */
  private readonly edgeToPredicates: Record<number, Set<number>> = {};
  /**
   * Data structure storing the connections of the graph.
   */
  private readonly edgeDict: Record<number, number[]> = {};

  public constructor() {}

  public traverse(start: string, reachableEdges: string[]): K[] {
    throw new Error('Method not implemented.');
  }

  public attachStatisticListener(statistics: IStatisticBase<IDiscoverEventData>[]) {
    if (statistics.length > 1) {
      throw new Error('TraversalCacheIndex cannot listen to more than one statistic');
    }
    statistics[0].on(this.addDiscoveryToCache);
  }

  public delete(node: K) {
    const nodeIndex = this.nodeToIndex[node];
    if (!nodeIndex) {
      return false;
    }
    // TODO Implement deletion logic here, likely with re-connecting
    // old connections with new predicate annotations
    return true;
  }

  public clear() {
    throw new Error('Method not implemented.');
  }

  private addDiscoveryToCache(discoveryData: IDiscoverEventData) {
    console.log(discoveryData);
  }
}

export interface ITraversalIndex<K extends {}> {
  /**
   * Traverses the index to find all reachable nodes given a start node
   * and acceptable edge types.
   * @param start
   * @param reachableEdges
   */
  traverse: (start: string, reachableEdges: string[]) => K[];

  /**
   * Allows the index to attach listeners to statistic events emitted by the engine.
   * These can be used to build the index and must be re-set for every query.
   */
  attachStatisticListener: (statistics: IStatisticBase<any>[]) => void;

  /**
   * Deletes a node from the traversal index.
   */
  delete: (node: K) => boolean;

  /**
   * Empties the traversal index
   */
  clear: () => void;
}
