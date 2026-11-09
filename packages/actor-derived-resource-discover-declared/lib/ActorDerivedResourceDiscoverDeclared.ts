import type {
  IDerivedResourceRaw,
  IDerivedResourceUnidentified,
} from '@comunica/actor-extract-links-solid-derived-resources';
import type { MediatorDereference } from '@comunica/bus-dereference';
import type {
  IActionDerivedResourceDiscover,
  IActorDerivedResourceDiscoverArgs,
  IActorDerivedResourceDiscoverOutput,
} from '@comunica/bus-derived-resource-discover';
import { ActorDerivedResourceDiscover } from '@comunica/bus-derived-resource-discover';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import type * as RDF from '@rdfjs/types';

/**
 * The vocabulary of Solid derived resource descriptions.
 */
export const DERIVED_RESOURCES = 'urn:npm:solid:derived-resources:';

/**
 * A comunica Declared Derived Resource Discover Actor.
 *
 * Reads the derived resources a description declares with `dr:derivedResource`, each with its
 * template, selectors and filter. A filter is either held by the description or pointed to, in which
 * case it is fetched, so the resources come with their filters.
 *
 * Only descriptions are read: a Solid server includes a container's description in the container
 * itself, and reading the declarations from there as well would take every resource up twice.
 */
export class ActorDerivedResourceDiscoverDeclared extends ActorDerivedResourceDiscover {
  public readonly mediatorDereference: MediatorDereference;

  public constructor(args: IActorDerivedResourceDiscoverDeclaredArgs) {
    super(args);
    this.mediatorDereference = args.mediatorDereference;
  }

  public async test(action: IActionDerivedResourceDiscover): Promise<TestResult<IActorTest>> {
    if (action.kind !== 'description') {
      return failTest(`${this.name} only reads the derived resources declared in descriptions`);
    }
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourceDiscover): Promise<IActorDerivedResourceDiscoverOutput> {
    const quads = await ActorDerivedResourceDiscover.collectQuads(
      action.metadata,
      quad => quad.predicate.value.startsWith(DERIVED_RESOURCES),
    );
    const declared = this.readDeclarations(quads, action.describedUrl);
    if (declared.length === 0) {
      return { 
        discovered: [],
      };
    }
    return {
      discovered: [{
        scope: action.describedUrl,
        producedBy: this.name,
        resources: Promise.all(declared.map(resource => this.resolveFilter(resource, action.context))),
      }],
    };
  }

  /**
   * The derived resources the quads declare.
   * @param quads The quads of the derived resources vocabulary.
   * @param describedUrl The container the description belongs to, which the templates resolve against.
   */
  public readDeclarations(quads: RDF.Quad[], describedUrl: string): IDerivedResourceRaw[] {
    // The description is a handful of triples, so it is read directly rather than queried
    const valuesOf = (resource: RDF.Term, predicate: string): RDF.Term[] => quads
      .filter(quad => quad.predicate.value === DERIVED_RESOURCES + predicate && quad.subject.equals(resource))
      .map(quad => quad.object);

    const resources: IDerivedResourceRaw[] = [];
    const seen: RDF.Term[] = [];
    for (const declaration of quads.filter(quad => quad.predicate.value === `${DERIVED_RESOURCES}derivedResource`)) {
      const resource = declaration.object;
      if (seen.some(term => term.equals(resource))) {
        continue;
      }
      seen.push(resource);

      const [ template ] = valuesOf(resource, 'template');
      const [ filter ] = valuesOf(resource, 'filter');
      const selectors = valuesOf(resource, 'selector');
      // A resource missing any of these cannot be used
      if (!template || !filter || selectors.length === 0) {
        continue;
      }
      resources.push({
        baseUrl: describedUrl,
        template: template.value,
        selectors: selectors.map(selector => selector.value),
        // The filter is either a literal holding it or the resource it can be read from
        ...filter.termType === 'Literal' ? { filter: filter.value } : { filterUri: { url: filter.value }},
      });
    }
    return resources;
  }

  /**
   * The resource with its filter, fetched from where it points to when the description does not hold it.
   */
  public async resolveFilter(
    resource: IDerivedResourceRaw,
    context: IActionContext,
  ): Promise<IDerivedResourceUnidentified> {
    if (resource.filter !== undefined) {
      return { ...resource, filter: resource.filter.trim() };
    }
    if (!resource.filterUri) {
      throw new Error(`Derived resource ${resource.template} has no filter to dereference`);
    }
    const response = await this.mediatorDereference.mediate({
      url: resource.filterUri.url,
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
    const filter = await streamToString(response.data);
    return { ...resource, filter: filter.trim() };
  }
}

function streamToString(stream: NodeJS.ReadableStream): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', chunk => chunks.push(Buffer.from(chunk)));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
  });
}

export interface IActorDerivedResourceDiscoverDeclaredArgs extends IActorDerivedResourceDiscoverArgs {
  /**
   * The dereference mediator fetching the filters a description points to.
   */
  mediatorDereference: MediatorDereference;
}
