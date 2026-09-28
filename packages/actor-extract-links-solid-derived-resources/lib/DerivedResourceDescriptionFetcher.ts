import type { IActorDereferenceOutput, MediatorDereference } from '@comunica/bus-dereference';
import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import { KeysDerivedResourceIdentify } from '@comunica/context-entries-link-traversal';
import type { IActionContext } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import type { IDerivedResourceRaw, IDerivedResourceUnidentified } from './ActorExtractLinksSolidDerivedResources';

const DR = 'urn:npm:solid:derived-resources:';

/**
 * Fetches a derived resource description document together with the filters of every resource it
 * declares: everything about a pod's derived resources that can be known before identifying them.
 */
export class DerivedResourceDescriptionFetcher {
  public constructor(
    protected readonly mediatorDereferenceRdf: MediatorDereferenceRdf,
    protected readonly mediatorDereference: MediatorDereference,
  ) {}

  /**
   * The resources a description declares, fetched once per query: a description that was already
   * requested, whether by another document or ahead of the seed, is not requested again.
   */
  public get(url: string, context: IActionContext): Promise<IDerivedResourceUnidentified[]> {
    const descriptions = context.get(KeysDerivedResourceIdentify.derivedResourceDescriptions);
    const known = descriptions?.get(url);
    if (known) {
      return known;
    }
    const fetched = this.fetch(url, context);
    descriptions?.set(url, fetched);
    return fetched;
  }

  public async fetch(url: string, context: IActionContext): Promise<IDerivedResourceUnidentified[]> {
    const resources = await this.dereferenceDescription(url, context);
    return Promise.all(resources.map(resource => this.dereferenceFilter(resource, context)));
  }

  /**
   * The resources declared in a description, with their selectors gathered per resource.
   */
  public async dereferenceDescription(url: string, context: IActionContext): Promise<IDerivedResourceRaw[]> {
    const response = await this.mediatorDereferenceRdf.mediate({ url, context });
    const quads = await new Promise<RDF.Quad[]>((resolve, reject) => {
      const collected: RDF.Quad[] = [];
      response.data.on('data', (quad: RDF.Quad) => collected.push(quad));
      response.data.on('error', reject);
      response.data.on('end', () => resolve(collected));
    });

    // The description is a handful of triples, so it is read directly rather than queried
    const valuesOf = (resource: RDF.Term, predicate: string): RDF.Term[] => quads
      .filter(quad => quad.predicate.value === DR + predicate && quad.subject.equals(resource))
      .map(quad => quad.object);

    const baseUrl = url.split('.meta')[0];
    const resources: IDerivedResourceRaw[] = [];
    const seen: RDF.Term[] = [];
    for (const declaration of quads.filter(quad => quad.predicate.value === `${DR}derivedResource`)) {
      const resource = declaration.object;
      if (seen.some(term => term.equals(resource))) {
        continue;
      }
      seen.push(resource);

      const [ template ] = valuesOf(resource, 'template');
      const [ filter ] = valuesOf(resource, 'filter');
      const selectors = valuesOf(resource, 'selector');
      // A resource missing any of these cannot be used, as before when all three were required
      if (!template || !filter || selectors.length === 0) {
        continue;
      }
      resources.push({
        baseUrl,
        template: template.value,
        selectors: selectors.map(selector => selector.value),
        filterUri: { url: filter.value },
      });
    }
    return resources;
  }

  public async dereferenceFilter(
    derivedResourcesUnidentified: IDerivedResourceRaw,
    context: IActionContext,
  ): Promise<IDerivedResourceUnidentified> {
    const response: IActorDereferenceOutput = await this.mediatorDereference.mediate({
      url: derivedResourcesUnidentified.filterUri.url,
      acceptErrors: true,
      method: 'GET',
      // We use the headers to ensure the server knows we are looking for RDF
      headers: new Headers({
        Accept: 'text/turtle,application/n-quads,application/trig,application/ld+json,application/sparql-query',
      }),
      mediaTypes: async() => ({
        // SHACL-based filters use turtle
        'text/turtle': 1,
        // Quad pattern indexes are represented as json
        'application/json': 0.4,
        // SPARQL-based filters
        'application/sparql-query': 0.7,
        // QPF uses plain text filter files
        'text/plain': 0.4,
        // Fallback (TODO: Should we even have this?)
        '*/*': 0.1,
      }),
      context,
    });
    const rawText = await this.streamToString(response.data);
    return { ...derivedResourcesUnidentified, filter: rawText.trim() };
  }

  private streamToString(stream: NodeJS.ReadableStream): Promise<string> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      stream.on('data', chunk => chunks.push(Buffer.from(chunk)));
      stream.on('error', err => reject(err));
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });
  }
}
