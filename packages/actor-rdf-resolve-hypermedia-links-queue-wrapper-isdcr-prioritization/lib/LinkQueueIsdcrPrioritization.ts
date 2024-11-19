import type { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import type { ILink } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueWrapper } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';
import type { PartialResult } from '@comunica/types';
import type * as RDF from '@rdfjs/types';

/**
 * A link queue that changes priorities based on indegree of nodes.
 */
export class LinkQueueIsdcrPrioritization extends LinkQueueWrapper<LinkQueuePriority> {
  public priorities: Record<string, number> = {};

  public constructor(linkQueue: LinkQueuePriority, intermediateResults: StatisticIntermediateResults) {
    super(linkQueue);
    intermediateResults.on(data => this.processIntermediateResult(data));
  }

  public override push(link: ILink, parent: ILink): boolean {
    // Seed URLs without parent get priority zero
    const parentPriority = (this.priorities[parent.url] ?? 1) - 1;
    if (!this.priorities[link.url] || parentPriority > this.priorities[link.url]) {
      this.priorities[link.url] = parentPriority;
    }
    link.metadata = {
      ...link.metadata,
      priority: this.priorities[link.url],
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
    if (result.type === 'bindings') {
      const resultSize = result.data.size;
      result.data.forEach((binding: RDF.Term, _: RDF.Variable) => {
        if (binding.termType === 'NamedNode') {
          const url = new URL(binding.value);
          const normalized = url.origin + url.pathname;
          if (!this.priorities[normalized] || resultSize > this.priorities[normalized]) {
            this.priorities[normalized] = resultSize;
            this.linkQueue.setPriority(normalized, resultSize);
          }
        }
      });
    }
  }
}
