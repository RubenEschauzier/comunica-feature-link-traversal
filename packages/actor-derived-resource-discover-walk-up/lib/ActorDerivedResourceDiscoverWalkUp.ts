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
 * A comunica Walk Up Derived Resource Discover Actor.
 *
 * Walks up from a document to the containers it lies in, and discovers the resources their
 * descriptions (`.meta`) link to, as a description. A pod rarely links its own storage root from its
 * documents, while the root's description is where it names what answers queries over it, such as a
 * SPARQL endpoint. So every document leads to the descriptions of the containers above it.
 */
export class ActorDerivedResourceDiscoverWalkUp extends ActorDerivedResourceDiscover {
  public readonly maxDepth: number;
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  public readonly mediatorDerivedResourceDiscover: MediatorDerivedResourceDiscover;

  public constructor(args: IActorDerivedResourceDiscoverWalkUpArgs) {
    super(args);
    this.maxDepth = args.maxDepth;
    this.mediatorDereferenceRdf = args.mediatorDereferenceRdf;
    this.mediatorDerivedResourceDiscover = args.mediatorDerivedResourceDiscover;
  }

  public async test(action: IActionDerivedResourceDiscover): Promise<TestResult<IActorTest>> {
    if (action.kind !== 'document') {
      return failTest(`${this.name} only walks up from traversed documents`);
    }
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourceDiscover): Promise<IActorDerivedResourceDiscoverOutput> {
    const discovered: IDiscoveredResources[] = [];
    for (const url of this.containerDescriptions(action.url)) {
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
    }``
    return { 
      discovered,
    };
  }

  /**
   * The descriptions of the containers the document lies in, outermost first, down to the configured
   * depth. The server root is not one of them: it holds every pod on the server rather than one.
   * @param document The url of the document.
   */
  public containerDescriptions(document: string): string[] {
    let url: URL;
    try {
      url = new URL(document);
    } catch {
      return [];
    }
    // The containers are the segments before the last, which is the document itself, also when that
    // is a container (and the last segment is empty, after its trailing slash)
    const segments = url.pathname.split('/').slice(1);
    if (segments.at(-1) === '') {
      segments.pop();
    }
    segments.pop();
    const descriptions: string[] = [];
    for (let depth = 1; depth <= Math.min(segments.length, this.maxDepth); depth++) {
      descriptions.push(`${url.origin}/${segments.slice(0, depth).join('/')}/.meta`);
    }
    return descriptions;
  }
}

export interface IActorDerivedResourceDiscoverWalkUpArgs extends IActorDerivedResourceDiscoverArgs {
  /**
   * How deep the containers lie whose descriptions are read, in path segments: 1 is a container right
   * below the server root, such as `/producer2/`. Storages deeper than that are not found, while every
   * level costs a failed request for each container on it that has no description.
   * @range {integer}
   * @default {1}
   */
  maxDepth: number;
  /**
   * The mediator fetching the descriptions.
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * The mediator discovering the resources in a description, which is the one of this bus.
   */
  mediatorDerivedResourceDiscover: MediatorDerivedResourceDiscover;
}
