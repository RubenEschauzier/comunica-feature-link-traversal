import type { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import type { ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';
import type {
  ITopologyUpdateRccResult,
  ITopologyUpdateRccUpdate,
  StatisticTraversalTopologyRcc,
  TopologyUpdateRccEmit,
} from '@comunica/statistic-traversal-topology-rcc';
import type { PartialResult } from '@comunica/types';
import type * as RDF from '@rdfjs/types';

/**
 * A link queue that changes priorities based on indegree of nodes.
 */
export class LinkQueueIsRel2Prioritization extends LinkQueueWrapper<LinkQueuePriority> {
  public rccTopology: StatisticTraversalTopologyRcc;

  public adjacencyListOut: Record<number, number[]> = {};
  public adjacencyListIn: Record<number, number[]> = {};

  public isScores: Record<number, number> = {};
  public rel2Scores: Record<number, number> = {};

  public inNeighbourHoodNodes: Map<number, Set<number>> = new Map();
  public indexToNodeDict: Record<number, string> = {};
  public nodeToIndexDict: Record<string, number> = {};

  public constructor(linkQueue: LinkQueuePriority, rccTopology: StatisticTraversalTopologyRcc, intermediateResults: StatisticIntermediateResults) {
    super(linkQueue);
    rccTopology.on((update: TopologyUpdateRccEmit) => this.processTopologyUpdate(update));
    intermediateResults.on((update: PartialResult) => {
      this.processIntermediateResult(update);
    });
  }

  public override push(link: ILink, parent: ILink): boolean {
    // Default priority = 0, but if either the rcc score or the is score > 0
    // we use that
    let priority = 0;
    const id = this.nodeToIndexDict[link.url];
    if (this.rel2Scores[id] || this.isScores[id]) {
      priority = this.priority(id);
    }
    link.metadata = {
      ...link.metadata,
      priority,
    };
    return super.push(link, parent);
  }

  public override pop(): ILink | undefined {
    return super.pop(); ;
  }

  public override peek() {
    return super.peek();
  }

  public processIntermediateResult(result: PartialResult) {
    if (result.type === 'bindings' && result.metadata.operation === 'inner') {
      const resultSize = result.data.size;
      result.data.forEach((binding: RDF.Term, _) => {
        if (binding.termType === 'NamedNode') {
          const url = new URL(binding.value);
          const normalized = url.origin + url.pathname;
          const id = this.nodeToIndexDict[normalized];
          if (!this.isScores[id] || resultSize > this.isScores[id]) {
            this.isScores[id] = resultSize;
            this.linkQueue.setPriority(
              normalized,
              resultSize * (this.rel2Scores[id] || 1),
            );
          }
        }
      });
    }
  }

  public processTopologyUpdate(data: TopologyUpdateRccEmit) {
    if (data.updateType == 'discover') {
      this.processDiscovery(data);
    }
    if (data.updateType == 'result') {
      this.processResultUpdate(data);
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
    this.rel2Scores[data.parentNode] ??= 0;

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
    this.rel2Scores[data.childNode] = (this.rel2Scores[data.childNode] ?? 0) + twoStepRel;

    // Update priority in the link queue if it changed
    if (twoStepRel > 0) {
      this.linkQueue.setPriority(
        this.indexToNodeDict[data.childNode], 
        this.priority(data.childNode));
    }
  }

  public processResultUpdate(data: ITopologyUpdateRccResult) {
    // If node associated with result has a result contribution of one, it has increased from 0
    // so we update all priorities accordingly
    if (data.nodeResultContribution[data.changedNode] === 1) {
      const incremented = new Set<number>();
      const neighbours = this.adjacencyListOut[data.changedNode];
      if (neighbours) {
        for (const neighbour of neighbours) {
          if (!incremented.has(neighbour)) {
            this.rel2Scores[neighbour]++;
            this.linkQueue.setPriority(
              this.indexToNodeDict[neighbour],
              this.priority(neighbour),
            );    
            incremented.add(neighbour);
          }
          // Second-degree neighbours (indirect neighbours)
          const secondDegreeNeighbours = this.adjacencyListOut[neighbour];
          if (secondDegreeNeighbours) {
            for (const secondDegreeNeighbor of secondDegreeNeighbours) {
              if (!incremented.has(secondDegreeNeighbor)) {
                this.rel2Scores[secondDegreeNeighbor]++;
                this.linkQueue.setPriority(
                  this.indexToNodeDict[secondDegreeNeighbor],
                  this.priority(secondDegreeNeighbor),
                );
                incremented.add(secondDegreeNeighbor);
              }
            }
          }
        }
      }
    }
  }

  public priority(node: number){
    return (this.rel2Scores[node] || 1) * (this.isScores[node] || 1)
  }

}
