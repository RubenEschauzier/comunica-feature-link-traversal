import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { ActionContextKey } from '@comunica/core';
import { StatisticBase } from '@comunica/statistic-base';
import type { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import type { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import type { IDiscoverEventData, ILink, IStatisticBase } from '@comunica/types';

export class StatisticTraversalTopology extends StatisticBase<ITopologyUpdate> {
  public key: ActionContextKey<IStatisticBase<ITopologyUpdate>>;

  /**
   * Incoming and outgoing edges of nodes
   */
  public adjacencyListIn: Record<number, number[]>;
  public adjacencyListOut: Record<number, number[]>;

  /**
   * Nodes that are not yet dereferenced
   */
  public openNodes: number[];
  public edges: Set<string>;

  public nodeMetadata: Record<number, INodeMetadata>;
  public nodeToIndexDict: Record<string, number>;
  public indexToNodeDict: Record<number, string>;

  public nDereferenced = 0;

  public constructor(
    statisticLinkDiscovery: StatisticLinkDiscovery,
    statisticLinkDereference: StatisticLinkDereference,
  ) {
    super();
    this.key = KeysStatisticsTraversal.traversalTopology;

    this.adjacencyListIn = {};
    this.adjacencyListOut = {};

    this.edges = new Set();

    this.nodeMetadata = {};
    this.nodeToIndexDict = {};
    this.indexToNodeDict = {};

    this.openNodes = [];

    statisticLinkDereference.on((data: ILink) => {
      this.updateStatistic({
        type: 'dereference',
        data,
      });
    });
    statisticLinkDiscovery.on((data: IDiscoverEventData) => {
      this.updateStatistic({
        type: 'discover',
        data,
      });
    });
  }

  public updateStatistic(update: IDataTopologyUpdate): boolean {
    if (update.type === 'discover') {
      const child: ILink = {
        url: update.data.edge[1],
        metadata: update.data.metadataChild,
      };
      const parent: ILink = {
        url: update.data.edge[0],
        metadata: update.data.metadataParent,
      };
      const result = this.addEdge(child, parent);
      if (result) {
        this.emit({
          updateType: update.type,
          adjacencyListIn: this.adjacencyListIn,
          adjacencyListOut: this.adjacencyListOut,
          openNodes: this.openNodes,
          nodeToIndexDict: this.nodeToIndexDict,
          indexToNodeDict: this.indexToNodeDict,
          childNode: this.nodeToIndexDict[child.url],
          parentNode: this.nodeToIndexDict[parent.url],
        });
      }
    } else if (update.type === 'dereference') {
      const result = this.setDereferenced(update.data);
      if (result) {
        this.emit({
          updateType: update.type,
          adjacencyListIn: this.adjacencyListIn,
          adjacencyListOut: this.adjacencyListOut,
          openNodes: this.openNodes,
          nodeToIndexDict: this.nodeToIndexDict,
          indexToNodeDict: this.indexToNodeDict,
          childNode: this.nodeToIndexDict[update.data.url],
          parentNode: this.nodeToIndexDict[update.data.url],
        });
      }
    } else {
      throw new Error(`Invalid topology update data type passed`);
    }
    return true;
  }

  public addEdge(child: ILink, parent: ILink): boolean {
    // Self references edges are irrelevant
    if (child.url === parent.url) {
      return false;
    }
    /**
     * If the parent doesn't exist in the topology, it must be a seed URL
     * as these are not registered
     */
    if (this.nodeToIndexDict[parent.url] === undefined) {
      const seedParentId = this.nodeToId(parent.url);
      // We also initialize an empty incoming adjacency list for completeness. 
      this.adjacencyListIn[seedParentId] = [];
      this.nodeMetadata[seedParentId] = {
        seed: true,
        dereferenced: true,
        discoverOrder: [ -1 ],
        dereferenceOrder: -1,
      };
    }
    // Whether the child node is new
    let newNode = true;
    if (this.nodeToIndexDict[child.url]) {
      newNode = false;
    }
    const childId = this.nodeToId(child.url);
    const parentId = this.nodeToId(parent.url);

    // If new node we add it as an open node
    if (newNode) {
      this.openNodes.push(childId);
    }
    // Also set reverse dictionary
    this.indexToNodeDict[childId] = child.url;
    this.indexToNodeDict[parentId] = parent.url;

    // If edge already exits don't update
    if (this.edges.has(JSON.stringify([ parentId, childId ]))) {
      return false;
    }

    // Add to out adj list
    this.adjacencyListOut[parentId] = this.adjacencyListOut[parentId] ?
        [ ...this.adjacencyListOut[parentId], childId ] :
        [ childId ];

    // Add empty out adj list entry for child if newly discovered
    this.adjacencyListOut[childId] ??= [];

    // Add to in adj list
    this.adjacencyListIn[childId] = this.adjacencyListIn[childId] ?
        [ ...this.adjacencyListIn[childId], parentId ] :
        [ parentId ];

    // Add to edges
    this.edges.add(JSON.stringify([ parentId, childId ]));

    // Update metadata
    if (this.nodeMetadata[childId]) {
      const discoverOrder = this.nodeMetadata[childId].discoverOrder;
      this.nodeMetadata[childId].discoverOrder = discoverOrder ?
          [ ...discoverOrder, this.edges.size ] :
          [ this.edges.size ];
    } else {
      this.nodeMetadata[childId] = {
        seed: false,
        dereferenced: false,
        discoverOrder: [ this.edges.size ],
        dereferenceOrder: Number.NEGATIVE_INFINITY,
      };
    }
    return true;
  }

  public setDereferenced(link: ILink): boolean {
    if (this.nodeMetadata[this.nodeToId(link.url)].dereferenced === true){
      return false;
    }
    this.nodeMetadata[this.nodeToId(link.url)].dereferenced = true;
    this.nodeMetadata[this.nodeToId(link.url)].dereferenceOrder = this.nDereferenced;
    // Remove dereferenced node from open nodes (TODO check correct implementation)
    this.openNodes = this.openNodes.filter(val => val != this.nodeToId(link.url));
    this.nDereferenced ++;
    return true;
  }

  private nodeToId(node: string): number {
    if (this.nodeToIndexDict[node] === undefined) {
      this.nodeToIndexDict[node] = Object.keys(this.nodeToIndexDict).length;
    }
    return this.nodeToIndexDict[node];
  }
}

export type IDataTopologyUpdate = {
  type: 'dereference';
  data: ILink;
} | {
  type: 'discover';
  data: IDiscoverEventData;
};

export interface ITopologyUpdate {
  /**
   * What type of update happened to the topology
   */
  updateType: 'discover' | 'dereference';
  /**
   * Main data structure showing the graph connections
   */
  adjacencyListIn: Record<number, number[]>;
  /**
   * Main data structure showing the graph connections
   */
  adjacencyListOut: Record<number, number[]>;
  /**
   * What nodes haven't been dereferenced yet
   */
  openNodes: number[];
  /**
   * Dictionary mapping URLs to node ids
   */
  nodeToIndexDict: Record<string, number>;
  /**
   * Reverse mapping dict
   */
  indexToNodeDict: Record<number, string>;
  /**
   * Child node of new update (for dereference this is just the dereferenced node)
   */
  childNode: number;
  /**
   * Parent node of new update (for dereference this is just the dereferenced node)
   */
  parentNode: number;
}

export interface INodeMetadata {
  seed: boolean;
  dereferenced: boolean;
  discoverOrder: number[];
  dereferenceOrder: number;
}