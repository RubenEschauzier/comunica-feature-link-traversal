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
  public adjacencyListIn: Record<number, number[]> = {};
  public adjacencyListOut: Record<number, number[]> = {};

  /**
   * Nodes that are not yet dereferenced
   */
  public openNodes: number[] = [];
  public edges: Set<string> = new Set();
  /**
   * Used to determine traversal order
   */
  public edgesInOrder: number[][] = [];

  public nodeMetadata: Record<number, INodeMetadata> = {};
  public nodeToIndexDict: Record<string, number> = {};
  public indexToNodeDict: Record<number, string> = {};

  public dereferenceOrder: number[] = [];
  public nDereferenced = 0;
  public nDiscovered = 0;

  public constructor(
    statisticLinkDiscovery: StatisticLinkDiscovery,
    statisticLinkDereference: StatisticLinkDereference,
  ) {
    super();
    this.key = KeysStatisticsTraversal.traversalTopology;
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
      const childUrl = new URL(update.data.edge[1]);
      const parentUrl = new URL(update.data.edge[0]);
      const child: ILink = {
        url: childUrl.origin + childUrl.pathname,
        metadata: update.data.metadataChild,
      };
      const parent: ILink = {
        url: parentUrl.origin + parentUrl.pathname,
        metadata: update.data.metadataParent,
      };
      const result = this.addEdge(child, parent);
      if (result) {
        this.emit({
          updateType: update.type,
          adjacencyListIn: this.adjacencyListIn,
          adjacencyListOut: this.adjacencyListOut,
          edgesInOrder: this.edgesInOrder,
          openNodes: this.openNodes,
          dereferenceOrder: this.dereferenceOrder,
          nodeToIndexDict: this.nodeToIndexDict,
          indexToNodeDict: this.indexToNodeDict,
          childNode: this.nodeToIndexDict[child.url],
          parentNode: this.nodeToIndexDict[parent.url],
        });
      }
    } else if (update.type === 'dereference') {
      const url = new URL(update.data.url);
      update.data.url = url.origin + url.pathname;
      const result = this.setDereferenced(update.data);
      if (result) {
        this.emit({
          updateType: update.type,
          adjacencyListIn: this.adjacencyListIn,
          adjacencyListOut: this.adjacencyListOut,
          edgesInOrder: this.edgesInOrder,
          openNodes: this.openNodes,
          dereferenceOrder: this.dereferenceOrder,
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
        ...parent.metadata,
      };
    }
    // Whether the child node is new
    let newNode = true;
    if (this.nodeToIndexDict[child.url]) {
      newNode = false;
    }
    const childId = this.nodeToId(child.url);
    const parentId = this.nodeToId(parent.url);

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
    this.edgesInOrder.push([ parentId, childId ]);

    // Update metadata
    if (this.nodeMetadata[childId]) {
      const discoverOrder = this.nodeMetadata[childId].discoverOrder;
      this.nodeMetadata[childId].discoverOrder = discoverOrder ?
          [ ...discoverOrder, this.nDiscovered ] :
          [ this.nDiscovered ];
    } else {
      this.nodeMetadata[childId] = {
        seed: false,
        dereferenced: false,
        discoverOrder: [ this.nDiscovered ],
        dereferenceOrder: Number.NEGATIVE_INFINITY,
        ...child.metadata,
      };
    }
    // If new node we add it as an open node
    if (newNode) {
      this.openNodes.push(childId);
    }
    this.nDiscovered++;
    return true;
  }

  public setDereferenced(link: ILink): boolean {
    if (this.nodeMetadata[this.nodeToId(link.url)].dereferenced) {
      return false;
    }
    this.nodeMetadata[this.nodeToId(link.url)].dereferenced = true;
    this.nodeMetadata[this.nodeToId(link.url)].dereferenceOrder = this.nDereferenced;
    this.dereferenceOrder.push(this.nodeToId(link.url));
    // Remove dereferenced node from open nodes (TODO check correct implementation)
    this.openNodes = this.openNodes.filter(val => val !== this.nodeToId(link.url));
    this.nDereferenced++;
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
   * Order of edges added to graph
   */
  edgesInOrder: number[][];
  /**
   * What nodes haven't been dereferenced yet
   */
  openNodes: number[];
  /**
   * Order of dereferencing
   */
  dereferenceOrder: number[];
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
