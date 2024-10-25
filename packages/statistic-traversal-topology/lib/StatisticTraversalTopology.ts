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

  public constructor(
    statisticLinkDiscovery: StatisticLinkDiscovery,
    statisticLinkDereference: StatisticLinkDereference,
  ) {
    super();
    this.key = KeysStatisticsTraversal.traversalTopology;

    this.adjacencyListIn = [];
    this.adjacencyListOut = [];

    this.edges = new Set();

    this.nodeMetadata = {};
    this.nodeToIndexDict = {};
    this.indexToNodeDict = {};

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
          indexToNodeDict: this.indexToNodeDict
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
          indexToNodeDict: this.indexToNodeDict
        });
      }
    } else {
      throw new Error(`Invalid topology update data type passed`);
    }
    return true;
  }

  private addEdge(child: ILink, parent: ILink): boolean {
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
      this.nodeMetadata[seedParentId] = {
        seed: true,
        dereferenced: true,
        discoverOrder: [ -1 ],
        dereferenceOrder: -1,
      };
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

  private setDereferenced(link: ILink): boolean {
    this.nodeMetadata[this.nodeToId(link.url)].dereferenced = true;
    // Remove dereferenced node from open nodes (TODO check correct implementation)
    this.openNodes = this.openNodes.filter((val) => val != this.nodeToId(link.url));
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
}

export interface INodeMetadata {
  seed: boolean;
  dereferenced: boolean;
  discoverOrder: number[];
  dereferenceOrder: number;
}
