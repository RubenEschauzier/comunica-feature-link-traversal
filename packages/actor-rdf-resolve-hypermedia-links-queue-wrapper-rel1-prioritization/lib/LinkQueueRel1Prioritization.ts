import type { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import type { ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type {
  ITopologyUpdateRccResult,
  ITopologyUpdateRccUpdate,
  StatisticTraversalTopologyRcc,
  TopologyUpdateRccEmit,
} from '@comunica/statistic-traversal-topology-rcc';

/**
 * A link queue that changes priorities based on indegree of nodes.
 */
export class LinkQueueRel1Prioritization extends LinkQueueWrapper<LinkQueuePriority> {
  public rccTopology: StatisticTraversalTopologyRcc;

  public adjacencyListOut: Record<number, number[]> = {};
  public adjacencyListIn: Record<number, number[]> = {};

  public priorities: Record<number, number> = {};

  public indexToNodeDict: Record<number, string> = {};
  public nodeToIndexDict: Record<string, number> = {};

  public constructor(linkQueue: LinkQueuePriority, rccTopology: StatisticTraversalTopologyRcc) {
    super(linkQueue);
    rccTopology.on((update: TopologyUpdateRccEmit) => this.processTopologyUpdate(update));
  }

  public override push(link: ILink, parent: ILink): boolean {
    link.metadata = {
      ...link.metadata,
      priority: this.priorities[this.nodeToIndexDict[link.url]] ?? 0,
    };
    return super.push(link, parent);
  }

  public override pop(): ILink | undefined {
    return super.pop(); ;
  }

  public override peek() {
    return super.peek();
  }

  public processTopologyUpdate(data: TopologyUpdateRccEmit) {
    if (data.updateType == 'discover') {
      this.processDiscovery(data);
    }
    if (data.updateType == 'result') {
      this.processResult(data);
    }
  }

  public processDiscovery(data: ITopologyUpdateRccUpdate) {
    this.adjacencyListOut = data.adjacencyListOut;
    this.adjacencyListIn = data.adjacencyListIn;
    this.indexToNodeDict = data.indexToNodeDict;
    this.nodeToIndexDict = data.nodeToIndexDict;

    // If seed node we set rcc to zero to initialize
    this.priorities[data.parentNode] ??= 0;

    // On new discovery, we update child node with parent's rcc if its > 0
    this.priorities[data.childNode] = (this.priorities[data.childNode] ?? 0) +
      (data.nodeResultContribution[data.parentNode] > 0 ? 1 : 0);

    // Update the priority if it changed
    if (data.nodeResultContribution[data.parentNode] > 0) {
      this.linkQueue.setPriority(
        this.indexToNodeDict[data.childNode],
        this.priorities[data.childNode],
      );
    }
  }

  public processResult(data: ITopologyUpdateRccResult) {
    // When nodeResultContribution of this node is 1, it means we update priorities.
    // Otherwise it has already contributed to the rel score of neighbours
    if (data.nodeResultContribution[data.changedNode] === 1) {
      const neighbours = this.adjacencyListOut[data.changedNode];
      for (const neighbour of neighbours) {
        this.priorities[neighbour]++;
        this.linkQueue.setPriority(this.indexToNodeDict[neighbour], this.priorities[neighbour]);
      }
    }
  }
}
