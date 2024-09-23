import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { ActionContextKey } from '@comunica/core';
import { StatisticBase } from '@comunica/statistic-base';
import type { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import type { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import type { IDiscoverEventData, ILink, IStatisticBase } from '@comunica/types';

export class StatisticTraversalTopology extends StatisticBase<ITopologyUpdate> {
  public key: ActionContextKey<IStatisticBase<ITopologyUpdate>>;

  public adjacencyList: Record<number, number[]>;
  public traversalOrder: number[];
  public edges: Set<string>;

  public nodeMetadata: Record<number, INodeMetadata>;
  public nodeToIndexDict: Record<string, number>;

  public constructor(
    statisticLinkDiscovery: StatisticLinkDiscovery,
    statisticLinkDereference: StatisticLinkDereference,
  ) {
    super();
    this.key = KeysStatisticsTraversal.traversalTopology;

    this.adjacencyList = [];
    this.traversalOrder = [];
    this.edges = new Set();

    this.nodeMetadata = {};
    this.nodeToIndexDict = {};

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
          adjacencyList: this.adjacencyList,
          traversalorder: this.traversalOrder,
          edges: this.edges,
          nodeMetadata: this.nodeMetadata,
          nodeToIndexDict: this.nodeToIndexDict,
        });
      }
    } else if (update.type === 'dereference') {
      const result = this.setDereferenced(update.data);
      if (result) {
        this.emit({
          adjacencyList: this.adjacencyList,
          traversalorder: this.traversalOrder,
          edges: this.edges,
          nodeMetadata: this.nodeMetadata,
          nodeToIndexDict: this.nodeToIndexDict,
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

    // If edge already exits don't update
    if (this.edges.has(JSON.stringify([ parentId, childId ]))) {
      return false;
    }
    // Add to adj list
    this.adjacencyList[parentId] = this.adjacencyList[parentId] ?
        [ ...this.adjacencyList[parentId], childId ] :
        [ childId ];

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
    const linkId = this.nodeToId(link.url);

    this.traversalOrder.push(linkId);
    this.nodeMetadata[linkId].dereferenced = true;
    this.nodeMetadata[linkId].dereferenceOrder = this.traversalOrder.length - 1;

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
   * Main data structure showing the graph connections
   */
  adjacencyList: Record<number, number[]>;
  /**
   * Order in which links gets dereferenced
   */
  traversalorder: number[];
  /**
   * Edges in the graph
   */
  edges: Set<string>;
  /**
   * Metadata per node
   */
  nodeMetadata: Record<number, INodeMetadata>;
  /**
   * Dictionary mapping URLs to node ids
   */
  nodeToIndexDict: Record<string, number>;
}

export interface INodeMetadata {
  seed: boolean;
  dereferenced: boolean;
  discoverOrder: number[];
  dereferenceOrder: number;
}
