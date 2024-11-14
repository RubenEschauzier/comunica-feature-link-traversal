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
      priority = (this.rel2Scores[id] ?? 1) * (this.isScores[id] ?? 1);
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
              resultSize * (this.rel2Scores[id] ?? 1),
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

  public processDiscovery(data: ITopologyUpdateRccUpdate) {
    this.adjacencyListOut = data.adjacencyListOut;
    this.adjacencyListIn = data.adjacencyListIn;
    this.indexToNodeDict = data.indexToNodeDict;
    this.nodeToIndexDict = data.nodeToIndexDict;

    // If seed node we set rcc to zero to initialize
    this.rel2Scores[data.parentNode] ??= 0;

    let twoStepRel = data.nodeResultContribution[data.parentNode] > 0 ? 1 : 0;
    if (this.adjacencyListIn[data.parentNode]) {
      for (const secondDegreeNeighbor of this.adjacencyListIn[data.parentNode]) {
        twoStepRel += this.rel2Scores[secondDegreeNeighbor] > 0 ? 1 : 0;
      }
    }

    this.rel2Scores[data.childNode] = (this.rel2Scores[data.childNode] ?? 0) + twoStepRel;

    // Update the priority
    if (twoStepRel > 0) {
      this.linkQueue.setPriority(
        this.indexToNodeDict[data.childNode],
        this.rel2Scores[data.childNode] * (this.isScores[data.childNode] ?? 1),
      );
    }
  }

  public processResultUpdate(data: ITopologyUpdateRccResult) {
    if (data.nodeResultContribution[data.changedNode] === 1) {
      const neighbours = this.adjacencyListOut[data.changedNode];
      for (const neighbour of neighbours) {
        this.rel2Scores[neighbour]++;
        this.linkQueue.setPriority(
          this.indexToNodeDict[neighbour],
          this.rel2Scores[neighbour] * (this.isScores[neighbour] ?? 1),
        );
        if (this.adjacencyListOut[neighbour]) {
          for (const secondDegreeNeighbor of this.adjacencyListOut[neighbour]) {
            this.rel2Scores[secondDegreeNeighbor]++;
            this.linkQueue.setPriority(
              this.indexToNodeDict[secondDegreeNeighbor],
              this.rel2Scores[secondDegreeNeighbor] * (this.isScores[secondDegreeNeighbor] ?? 1),
            );
          }
        }
      }
    }
  }
}
