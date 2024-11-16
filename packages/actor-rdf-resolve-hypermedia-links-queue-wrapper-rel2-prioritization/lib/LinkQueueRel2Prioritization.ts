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
export class LinkQueueRel2Prioritization extends LinkQueueWrapper<LinkQueuePriority> {
  public rccTopology: StatisticTraversalTopologyRcc;

  public adjacencyListOut: Record<number, number[]> = {};
  public adjacencyListIn: Record<number, number[]> = {};

  public priorities: Record<number, number> = {};
  public inNeighbourHoodNodes: Map<number, Set<number>> = new Map();

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

  public processDiscovery(data: ITopologyUpdateRccUpdate): void {
    // Update adjacency lists and mappings
    Object.assign(this, {
      adjacencyListOut: data.adjacencyListOut,
      adjacencyListIn: data.adjacencyListIn,
      indexToNodeDict: data.indexToNodeDict,
      nodeToIndexDict: data.nodeToIndexDict,
    });

    // Ensure the parent node's priority is initialized
    this.priorities[data.parentNode] ??= 0;

    // Ensure in-neighborhood nodes are initialized
    for (const node of [ data.parentNode, data.childNode ]) {
      if (!this.inNeighbourHoodNodes.has(node)) {
        this.inNeighbourHoodNodes.set(node, new Set());
      }
    }

    // Prevent double counting of nodes in second-degree in-neighborhood
    const childInNeighbours = this.inNeighbourHoodNodes.get(data.childNode)!;
    let twoStepRel = data.nodeResultContribution[data.parentNode] > 0 ? 1 : 0;

    // Add parent node to child's in-neighbors
    childInNeighbours.add(data.parentNode);

    // Process second-degree neighbors
    const parentInNeighbours = this.adjacencyListIn[data.parentNode];
    if (parentInNeighbours) {
      for (const secondDegreeNeighbor of parentInNeighbours) {
        if (!childInNeighbours.has(secondDegreeNeighbor)) {
          if (data.nodeResultContribution[secondDegreeNeighbor] > 0) {
            twoStepRel++;
          }
          childInNeighbours.add(secondDegreeNeighbor);
        }
      }
    }

    // Update child node's priority
    const newPriority = (this.priorities[data.childNode] ?? 0) + twoStepRel;
    this.priorities[data.childNode] = newPriority;

    // Update priority in the link queue if it changed
    if (twoStepRel > 0) {
      this.linkQueue.setPriority(this.indexToNodeDict[data.childNode], newPriority);
    }
  }

  /**
   * Updates priority of all neighbours and second degree neighbours when new
   * result arrives
   * @param data Data from topology about the newly arrived result
   */
  public processResult(data: ITopologyUpdateRccResult) {
    // If node associated with result has a result contribution of one, it has increased from 0
    // so we update all priorities accordingly
    if (data.nodeResultContribution[data.changedNode] === 1) {
      const incremented = new Set<number>();
      const neighbours = this.adjacencyListOut[data.changedNode];
      if (neighbours) {
        for (const neighbour of neighbours) {
          if (!incremented.has(neighbour)) {
            this.incrementNode(neighbour);
            incremented.add(neighbour);
          }
          // Second-degree neighbours (indirect neighbours)
          const secondDegreeNeighbours = this.adjacencyListOut[neighbour];
          if (secondDegreeNeighbours) {
            for (const secondDegreeNeighbor of secondDegreeNeighbours) {
              if (!incremented.has(secondDegreeNeighbor)) {
                this.incrementNode(secondDegreeNeighbor);
                incremented.add(secondDegreeNeighbor);
              }
            }
          }
        }
      }
    }
  }

  private incrementNode(node: number) {
    this.priorities[node]++;
    this.linkQueue.setPriority(this.indexToNodeDict[node], this.priorities[node]);
  }
}
