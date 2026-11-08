import type { IDerivedResourceUnidentified } from '@comunica/actor-extract-links-solid-derived-resources';
import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import { KeysDerivedResourceIdentify, KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import type { IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Actor } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import type * as RDF from '@rdfjs/types';

/**
 * A comunica actor for derived-resource-discover events.
 *
 * Finds the resources that can answer (parts of) a query over a pod in the quads of a document, each
 * actor one kind of link to them: the derived resources a description declares, a SPARQL endpoint a
 * pod lists, a link to a description that declares more, ... A new kind of link is supported by
 * adding an actor to this bus, rather than by changing the actors that use what it finds.
 *
 * All actors that pass their test are run on the same metadata stream, and what they found is
 * concatenated. They read that stream as it comes, so an actor attaches to it in `run` before it awaits
 * anything, as the actors on the extract links bus do: once one of them reads it, quads that pass are
 * not seen by an actor that attaches later. {@link ActorDerivedResourceDiscover.collectQuads} does so.
 *
 * Actor types:
 * * Input:  IActionDerivedResourceDiscover:      the quads of a document, and what they describe.
 * * Test:   <none>
 * * Output: IActorDerivedResourceDiscoverOutput: the resources found, per part of the web they cover.
 *
 * @see IActionDerivedResourceDiscover
 * @see IActorDerivedResourceDiscoverOutput
 */
export abstract class ActorDerivedResourceDiscover<TS = undefined> extends Actor<
IActionDerivedResourceDiscover,
IActorTest,
IActorDerivedResourceDiscoverOutput,
TS
> {
  /**
   * @param args -
   *   \ @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   *   \ @defaultNested {No derived resource discover actor can read this document} busFailMessage
   */
  public constructor(args: IActorDerivedResourceDiscoverArgs<TS>) {
    super(args);
  }

  /**
   * The quads of the stream that are accepted, once the stream has ended. Attaches to the stream
   * immediately, so it is to be called before anything is awaited.
   * @param metadata The quads of the document.
   * @param accepts Whether a quad is to be kept. Only those are, as a document can be large.
   */
  public static collectQuads(metadata: RDF.Stream, accepts: (quad: RDF.Quad) => boolean): Promise<RDF.Quad[]> {
    return new Promise((resolve, reject) => {
      const quads: RDF.Quad[] = [];
      metadata.on('error', reject);
      metadata.on('data', (quad: RDF.Quad) => {
        if (accepts(quad)) {
          quads.push(quad);
        }
      });
      metadata.on('end', () => resolve(quads));
    });
  }

  /**
   * The resource a description describes: the container its `.meta` document belongs to.
   * @param description The url of the description.
   */
  public static describedUrlOf(description: string): string {
    return description.split('.meta')[0];
  }

  /**
   * The resources a description links to, with their filters. The description is fetched and its quads
   * are discovered on as a description, by whichever actors read descriptions.
   * @param url The url of the description.
   * @param context The action context.
   * @param mediatorDereferenceRdf The mediator fetching the description.
   * @param mediatorDerivedResourceDiscover The mediator discovering on the description.
   */
  public static async discoverDescription(
    url: string,
    context: IActionContext,
    mediatorDereferenceRdf: MediatorDereferenceRdf,
    mediatorDerivedResourceDiscover: MediatorDerivedResourceDiscover,
  ): Promise<IDerivedResourceUnidentified[]> {
    const { data } = await mediatorDereferenceRdf.mediate({ url, context });
    const { discovered } = await mediatorDerivedResourceDiscover.mediate({
      url,
      metadata: data,
      describedUrl: ActorDerivedResourceDiscover.describedUrlOf(url),
      kind: 'description',
      context,
    });
    // Covered by the description as a whole, which the document linking it is held back for
    return (await Promise.all(discovered.map(({ resources }) => resources))).flat();
  }

  /**
   * Takes up a description for the query, unless it already was: the resources it links to, over the
   * container it describes.
   *
   * Many documents can lead to the same description, so it is taken up by the first of them only, after
   * which it is kept off the crawl. It is fetched once as well, and not at all when it was already
   * fetched ahead of the seeds. It is claimed without awaiting anything, so a caller that has not
   * awaited anything since reading its document can be sure no other document takes it up as well.
   * @param url The url of the description.
   * @param context The action context.
   * @param mediatorDereferenceRdf The mediator fetching the description.
   * @param mediatorDerivedResourceDiscover The mediator discovering on the description.
   * @return The resources of the description, or undefined when it was already taken up.
   */
  public static takeUpDescription(
    url: string,
    context: IActionContext,
    mediatorDereferenceRdf: MediatorDereferenceRdf,
    mediatorDerivedResourceDiscover: MediatorDerivedResourceDiscover,
  ): IDiscoveredResources | undefined {
    const dynamicLinkFilter = context.get(KeysRdfResolveHypermediaLinks.dynamicFilter);
    if (dynamicLinkFilter?.matchesFilter(url)) {
      return;
    }
    dynamicLinkFilter?.addExact(url);

    const descriptions = context.get(KeysDerivedResourceIdentify.derivedResourceDescriptions);
    let resources = descriptions?.get(url);
    if (!resources) {
      // A description that cannot be read, such as one of a container that is no storage, links to nothing
      resources = ActorDerivedResourceDiscover.discoverDescription(
        url,
        context,
        mediatorDereferenceRdf,
        mediatorDerivedResourceDiscover,
      ).catch(() => []);
      descriptions?.set(url, resources);
    }
    return { scope: ActorDerivedResourceDiscover.describedUrlOf(url), resources };
  }
}

export interface IActionDerivedResourceDiscover extends IAction {
  /**
   * The url of the document the quads are of.
   */
  url: string;
  /**
   * The quads of the document, to be read as they come.
   */
  metadata: RDF.Stream;
  /**
   * The resource the quads describe, which what is found is read as covering (part of): the document
   * itself, or for a description, the container it belongs to.
   */
  describedUrl: string;
  /**
   * What kind of document the quads are of: one met during traversal, or a description of a pod that
   * one of those linked to.
   *
   * Some links are only read from one kind. A Solid server includes a container's description in the
   * container itself, so the derived resources a pod declares are read from its description alone,
   * as reading them from the container as well would take each of them up twice.
   */
  kind: DerivedResourceDocumentKind;
}

export type DerivedResourceDocumentKind = 'document' | 'description';

export interface IActorDerivedResourceDiscoverOutput extends IActorOutput {
  /**
   * The resources found, grouped by the part of the web they cover.
   */
  discovered: IDiscoveredResources[];
}

/**
 * Resources found in a document, and the part of the web they cover.
 *
 * The two become known at different times. That a document links to resources over some part of the
 * web is known once it has been read, and from then on links into that part can be held back, as
 * crawling it could answer the query a second time. What the resources are can take more requests,
 * such as fetching a description and the filters it points to.
 */
export interface IDiscoveredResources {
  /**
   * The url the resources cover the documents below of, such as the container of a pod.
   */
  scope: string;
  /**
   * The resources, with their filters.
   */
  resources: Promise<IDerivedResourceUnidentified[]>;
}

export type IActorDerivedResourceDiscoverArgs<TS = undefined> = IActorArgs<
IActionDerivedResourceDiscover,
IActorTest,
IActorDerivedResourceDiscoverOutput,
TS
>;

export type MediatorDerivedResourceDiscover = Mediate<
IActionDerivedResourceDiscover,
IActorDerivedResourceDiscoverOutput
>;
