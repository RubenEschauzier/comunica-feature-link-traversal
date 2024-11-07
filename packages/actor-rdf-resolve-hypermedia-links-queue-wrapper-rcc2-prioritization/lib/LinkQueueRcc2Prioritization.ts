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
export class LinkQueueRcc2Prioritization extends LinkQueueWrapper<LinkQueuePriority> {
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
    const result = super.pop();
    return result
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
    
    // On new discovery, we update child node with parent rcc and parents of parents rcc
    let twoStepRcc = data.nodeResultContribution[data.parentNode];
    // Calculate second degree in-neighbourhood
    if (this.adjacencyListIn[data.parentNode]){
      for (const secondDegreeNeighbor of this.adjacencyListIn[data.parentNode]) {
        // Default to zero as the childNode is also a second degree neighbour and it doesnt
        twoStepRcc += data.nodeResultContribution[secondDegreeNeighbor];
      }  
    }
    this.priorities[data.childNode] = (this.priorities[data.childNode] ?? 0) + twoStepRcc;

    if (twoStepRcc > 0) {
      this.linkQueue.setPriority(
        this.indexToNodeDict[data.childNode],
        this.priorities[data.childNode],
      );
    }
  }

  /**
   * Updates priority of all neighbours and second degree neighbours when new
   * result arrives
   * @param data Data from topology about the newly arrived result
   */
  public processResult(data: ITopologyUpdateRccResult) {
    const neighbours = this.adjacencyListOut[data.changedNode];
    for (const neighbour of neighbours) {
      this.priorities[neighbour]++;
      this.linkQueue.setPriority(this.indexToNodeDict[neighbour], this.priorities[neighbour]);
      if (this.adjacencyListOut[neighbour]) {
        for (const secondDegreeNeighbor of this.adjacencyListOut[neighbour]) {
          this.priorities[secondDegreeNeighbor]++;
          this.linkQueue.setPriority(this.indexToNodeDict[secondDegreeNeighbor], this.priorities[secondDegreeNeighbor]);
        }
      }
    }
  }
}
