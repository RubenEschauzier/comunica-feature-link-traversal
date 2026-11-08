import type {
  IActionDerivedResourceIdentify,
  IActorDerivedResourceIdentifyArgs,
  IActorDerivedResourceIdentifyOutput,
} from '@comunica/bus-derived-resource-identify';
import { ActorDerivedResourceIdentify } from '@comunica/bus-derived-resource-identify';
import type { MediatorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import { KeysInitQuery } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { ArrayIterator } from 'asynciterator';
import { QuerySourceSparqlEndpoint } from './QuerySourceSparqlEndpoint';

/**
 * A comunica Sparql Endpoint Derived Resource Identify Actor.
 *
 * Identifies a SPARQL endpoint from a discovered derived resource.
 */
export class ActorDerivedResourceIdentifySparqlEndpoint extends ActorDerivedResourceIdentify {
  public readonly mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;
  public readonly interfaceTypes: string[];

  public constructor(args: IActorDerivedResourceIdentifySparqlEndpointArgs) {
    super(args);
    this.mediatorQuerySourceIdentifyHypermedia = args.mediatorQuerySourceIdentifyHypermedia;
    this.interfaceTypes = args.interfaceTypes;
  }

  public async test(action: IActionDerivedResourceIdentify): Promise<TestResult<IActorTest>> {
    const { interfaceType } = action.derivedResourceUnidentified;
    if (!interfaceType || !this.interfaceTypes.includes(interfaceType)) {
      return failTest(`${this.name} can only identify interfaces linked as SPARQL endpoint`);
    }
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourceIdentify): Promise<IActorDerivedResourceIdentifyOutput> {
    const resource = action.derivedResourceUnidentified;
    const url = new URL(resource.template, resource.baseUrl).href;

    const { source } = await this.mediatorQuerySourceIdentifyHypermedia.mediate({
      url,
      metadata: {},
      quads: new ArrayIterator([], { autoStart: false }),
      forceSourceType: 'sparql',
      context: action.context,
    });
    const dataFactory = action.context.getSafe(KeysInitQuery.dataFactory);
    const querySource = new QuerySourceSparqlEndpoint(url, source, new AlgebraFactory(dataFactory), dataFactory);

    return {
      derivedResourceIdentified: {
        iri: url,
        ...resource,
        derivedResourceSelectorShape: await querySource.getSelectorShape(action.context),
        querySource,
        // TODO: placeholders like those of the other resources. An endpoint answers what it is asked
        // exactly and in one request, but evaluates all of it on the server
        resourceCoefficients: {
          selectivity: 1,
          requests: 1,
          compute: 5,
        },
      },
    };
  }
}

export interface IActorDerivedResourceIdentifySparqlEndpointArgs extends IActorDerivedResourceIdentifyArgs {
  /**
   * The mediator creating the SPARQL source of the endpoint.
   */
  mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;
  /**
   * The predicates that link a dataset to a SPARQL endpoint, as passed on by the reader of the link.
   * @default {http://rdfs.org/ns/void#sparqlEndpoint}
   */
  interfaceTypes: string[];
}
