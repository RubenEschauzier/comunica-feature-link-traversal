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
    // On new discovery, we update child node with parent rcc and parents of parents rcc
    let twoStepRel = data.nodeResultContribution[data.parentNode] > 0 ? 1 : 0;
    if (this.adjacencyListIn[data.parentNode]){
      for (const secondDegreeNeighbor of this.adjacencyListIn[data.parentNode]) {
        twoStepRel += this.priorities[secondDegreeNeighbor] > 0 ? 1 : 0;
      }  
    }

    this.priorities[data.childNode] = (this.priorities[data.childNode] ?? 0) + twoStepRel;

    // Update the priority if it changed
    if (twoStepRel > 0) {
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
    // If node associated with result has a result contribution of one, it has increased from 0
    // so we update all priorities accordingly
    if (data.nodeResultContribution[data.changedNode] === 1) {
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
}
