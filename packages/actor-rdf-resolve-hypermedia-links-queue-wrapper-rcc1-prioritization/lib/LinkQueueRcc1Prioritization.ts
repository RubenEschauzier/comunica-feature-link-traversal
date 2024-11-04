import type { ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { 
  ITopologyUpdateRccResult,
   ITopologyUpdateRccUpdate,
    StatisticTraversalTopologyRcc, 
    TopologyUpdateRccEmit 
} from '@comunica/statistic-traversal-topology-rcc';

/**
 * A link queue that changes priorities based on indegree of nodes.
 */
export class LinkQueueRcc1Prioritization extends LinkQueueWrapper<LinkQueuePriority> {
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
      priority: this.priorities[this.nodeToIndexDict[link.url]] ?? 0 
    };
    return super.push(link, parent);
  }

  public override pop(): ILink | undefined {
    return super.pop();;
  }

  public override peek(){
    return super.peek();
  }

  public processTopologyUpdate(data: TopologyUpdateRccEmit){
    if(data.updateType == 'discover'){
      this.processDiscovery(data);
    }
    if(data.updateType == 'result'){
      this.processResult(data);
    }
  }

  public processDiscovery(data: ITopologyUpdateRccUpdate){
    this.adjacencyListOut = data.adjacencyListOut;
    this.adjacencyListIn = data.adjacencyListIn;
    this.indexToNodeDict = data.indexToNodeDict;
    this.nodeToIndexDict = data.nodeToIndexDict;
    // On new discovery, we update child node with parent's rcc if its > 0
    const parentRcc = data.nodeResultContribution[data.parentNode];
    if (parentRcc > 0){
      if (!this.priorities[data.childNode]){
        this.priorities[data.childNode] = parentRcc;
      }
      else{
        this.priorities[data.childNode] += parentRcc;
      }
      // Update the priority
      this.linkQueue.setPriority(
        this.indexToNodeDict[data.childNode], 
        this.priorities[data.childNode]
      );  
    }
  }

  public processResult(data: ITopologyUpdateRccResult){
    const neighbours = this.adjacencyListOut[data.changedNode];
    for (const neighbour of neighbours){
      this.priorities[neighbour]++;
      this.linkQueue.setPriority(this.indexToNodeDict[neighbour], this.priorities[neighbour]);
    }
  }
}
