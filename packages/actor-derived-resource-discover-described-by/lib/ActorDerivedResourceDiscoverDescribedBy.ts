import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type {
  IActionDerivedResourceDiscover,
  IActorDerivedResourceDiscoverArgs,
  IActorDerivedResourceDiscoverOutput,
  IDiscoveredResources,
  MediatorDerivedResourceDiscover,
} from '@comunica/bus-derived-resource-discover';
import { ActorDerivedResourceDiscover } from '@comunica/bus-derived-resource-discover';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';

/**
 * A comunica Described By Derived Resource Discover Actor.
 *
 * Follows a link from a document to the description of its pod, such as
 * `<profile/card#me> powder-s:describedby <.meta>`, and discovers the resources that description links
 * to by fetching it and discovering on it in turn, as a description.
 *
 * Every document of a pod can link the same description, so it is taken up once per query: by the
 * first document that links it, after which it is kept off the crawl. It is fetched once as well, and
 * not at all when it was already fetched ahead of the seeds. Links are only followed from documents
 * met during traversal, not from descriptions, so descriptions linking further descriptions are not
 * followed indefinitely.
 */
export class ActorDerivedResourceDiscoverDescribedBy extends ActorDerivedResourceDiscover {
  public readonly predicates: string[];
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  public readonly mediatorDerivedResourceDiscover: MediatorDerivedResourceDiscover;

  public constructor(args: IActorDerivedResourceDiscoverDescribedByArgs) {
    super(args);
    this.predicates = args.predicates;
    this.mediatorDereferenceRdf = args.mediatorDereferenceRdf;
    this.mediatorDerivedResourceDiscover = args.mediatorDerivedResourceDiscover;
  }

  public async test(action: IActionDerivedResourceDiscover): Promise<TestResult<IActorTest>> {
    if (action.kind !== 'document') {
      return failTest(`${this.name} only follows links to descriptions from traversed documents`);
    }
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourceDiscover): Promise<IActorDerivedResourceDiscoverOutput> {
    const quads = await ActorDerivedResourceDiscover.collectQuads(
      action.metadata,
      quad => this.predicates.includes(quad.predicate.value) && quad.object.termType === 'NamedNode',
    );
    // Taken up without awaiting anything since reading the document, so no other document reading the
    // same description takes it up as well
    const discovered: IDiscoveredResources[] = [];
    for (const url of new Set(quads.map(quad => quad.object.value))) {
      const description = ActorDerivedResourceDiscover.takeUpDescription(
        url,
        this.name,
        action.context,
        this.mediatorDereferenceRdf,
        this.mediatorDerivedResourceDiscover,
      );
      if (description) {
        discovered.push(description);
      }
    }
    return { 
      discovered,
    };
  }
}

export interface IActorDerivedResourceDiscoverDescribedByArgs extends IActorDerivedResourceDiscoverArgs {
  /**
   * The predicates linking a document to the description of its pod.
   * @default {http://www.w3.org/2007/05/powder-s#describedby}
   */
  predicates: string[];
  /**
   * The mediator fetching the descriptions.
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * The mediator discovering the resources in a description, which is the one of this bus.
   */
  mediatorDerivedResourceDiscover: MediatorDerivedResourceDiscover;
}
