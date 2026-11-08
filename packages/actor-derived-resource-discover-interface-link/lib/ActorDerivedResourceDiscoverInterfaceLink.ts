import type { IDerivedResourceUnidentified } from '@comunica/actor-extract-links-solid-derived-resources';
import type {
  IActionDerivedResourceDiscover,
  IActorDerivedResourceDiscoverArgs,
  IActorDerivedResourceDiscoverOutput,
} from '@comunica/bus-derived-resource-discover';
import { ActorDerivedResourceDiscover } from '@comunica/bus-derived-resource-discover';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type * as RDF from '@rdfjs/types';

/**
 * The predicate linking a dataset to the SPARQL endpoint that answers queries over it.
 */
export const VOID_SPARQL_ENDPOINT = 'http://rdfs.org/ns/void#sparqlEndpoint';

/**
 * A comunica Interface Link Derived Resource Discover Actor.
 *
 * Reads the query interfaces a dataset is linked to, as resources answering over that dataset.
 * This does not have to be a derived resource, it can be a SPARQL endpoint declared with:
 * (`<data/> void:sparqlEndpoint <endpoint>`)
 */

export class ActorDerivedResourceDiscoverInterfaceLink extends ActorDerivedResourceDiscover {
  public readonly predicates: string[];

  public constructor(args: IActorDerivedResourceDiscoverInterfaceLinkArgs) {
    super(args);
    this.predicates = args.predicates;
  }

  public async test(_action: IActionDerivedResourceDiscover): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourceDiscover): Promise<IActorDerivedResourceDiscoverOutput> {
    const quads = await ActorDerivedResourceDiscover.collectQuads(
      action.metadata,
      quad => this.predicates.includes(quad.predicate.value),
    );
    return {
      discovered: this.readInterfaces(quads, action.describedUrl).map(resource => ({
        scope: resource.baseUrl,
        resources: Promise.resolve([ resource ]),
      })),
    };
  }

  /**
   * The interfaces the quads link the described resource to.
   * @param quads The quads with a configured predicate, as run collects them.
   * @param describedUrl The resource the quads describe.
   */
  public readInterfaces(quads: RDF.Quad[], describedUrl: string): IDerivedResourceUnidentified[] {
    const described = withoutHash(describedUrl);
    const resources = new Map<string, IDerivedResourceUnidentified>();
    for (const quad of quads) {
      if (quad.subject.termType !== 'NamedNode' ||
        quad.object.termType !== 'NamedNode' ||
        // Only acceptable to define a interface over its own url
        withoutHash(quad.subject.value) !== described) {
        continue;
      }
      const dataset = quad.subject.value;
      const key = `${quad.predicate.value} ${quad.object.value}`;
      if (!resources.has(key)) {
        resources.set(key, {
          baseUrl: dataset,
          template: quad.object.value,
          selectors: datasetSelectors(dataset),
          filter: '',
          interfaceType: quad.predicate.value,
        });
      }
    }
    return [ ...resources.values() ];
  }
}

/**
 * The documents a dataset covers: everything below it when it is a container. Of any other dataset it
 * is not known which documents it covers, so none are claimed.
 */
function datasetSelectors(dataset: string): string[] {
  return dataset.endsWith('/') ? [ `${dataset}**` ] : [];
}

function withoutHash(url: string): string {
  const hash = url.indexOf('#');
  return hash < 0 ? url : url.slice(0, hash);
}

export interface IActorDerivedResourceDiscoverInterfaceLinkArgs extends IActorDerivedResourceDiscoverArgs {
  /**
   * The predicates linking a dataset to a query interface over all of its data.
   * @default {http://rdfs.org/ns/void#sparqlEndpoint}
   */
  predicates: string[];
}
