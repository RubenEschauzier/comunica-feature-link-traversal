import type { MediatorExtractLinks } from '@comunica/bus-extract-links';
import type {
  IActionQuerySourceDereferenceLink,
  IActorQuerySourceDereferenceLinkArgs,
  IActorQuerySourceDereferenceLinkOutput,
  MediatorQuerySourceDereferenceLink,
} from '@comunica/bus-query-source-dereference-link';
import { ActorQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { KeysInitQuery } from '@comunica/context-entries';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContextKey, failTest, passTestVoid } from '@comunica/core';
import { QuerySourceExtractLinks } from './QuerySourceExtractLinks';

/**
 * A comunica query source dereference link actor that follows links in the answers of the known query sources.
 *
 * A query source given with a type, such as a SPARQL endpoint, is a known member of the federation. Link
 * traversal takes it up as a seed with that type forced, and queries it alongside the documents it reads.
 * Link traversal extracts links from those documents; this actor makes the links in the answers of a known
 * source extracted in the same way, through the extract links mediator, by wrapping the source the link
 * dereferences to.
 *
 * Only links with a forced source type are wrapped, which only seeds from the query sources have. Resources
 * found during traversal, such as derived resources, are identified by their own actors, which extract their
 * links already.
 */
export class ActorQuerySourceDereferenceLinkExtractLinks extends ActorQuerySourceDereferenceLink {
  public readonly mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;
  public readonly mediatorExtractLinks: MediatorExtractLinks;

  private static readonly keyWrapped = new ActionContextKey<boolean>(
    '@comunica/actor-query-source-dereference-link-extract-links:wrapped',
  );

  public constructor(args: IActorQuerySourceDereferenceLinkExtractLinksArgs) {
    super(args);
    this.mediatorQuerySourceDereferenceLink = args.mediatorQuerySourceDereferenceLink;
    this.mediatorExtractLinks = args.mediatorExtractLinks;
  }

  public async test(action: IActionQuerySourceDereferenceLink): Promise<TestResult<IActorTest>> {
    if (action.context.get(ActorQuerySourceDereferenceLinkExtractLinks.keyWrapped)) {
      return failTest(`${this.name} only wraps a source once`);
    }
    if (!action.link.forceSourceType) {
      return failTest(`${this.name} only wraps the sources of seeds given with a type`);
    }
    if (!action.context.has(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager)) {
      return failTest(`${this.name} requires link traversal`);
    }
    return passTestVoid();
  }

  public async run(action: IActionQuerySourceDereferenceLink): Promise<IActorQuerySourceDereferenceLinkOutput> {
    const output = await this.mediatorQuerySourceDereferenceLink.mediate({
      ...action,
      context: action.context.set(ActorQuerySourceDereferenceLinkExtractLinks.keyWrapped, true),
    });
    return {
      ...output,
      source: new QuerySourceExtractLinks(
        output.source,
        action.link.url,
        this.mediatorExtractLinks,
        action.context.getSafe(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager),
        action.context.getSafe(KeysInitQuery.dataFactory),
      ),
    };
  }
}

export interface IActorQuerySourceDereferenceLinkExtractLinksArgs extends IActorQuerySourceDereferenceLinkArgs {
  /**
   * The query source dereference link mediator, dereferencing the link to the source to wrap.
   */
  mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;
  /**
   * The extract links mediator, extracting links from the triples the answers matched.
   */
  mediatorExtractLinks: MediatorExtractLinks;
}
