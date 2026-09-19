import { ActorInitQueryBase, QueryEngineBase } from '@comunica/actor-init-query';
import { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import { ActorExtractLinks, IActionExtractLinks, IActorExtractLinksOutput, IActorExtractLinksArgs, IExtractPattern } from '@comunica/bus-extract-links';
import { IActorDereferenceOutput, MediatorDereference } from "@comunica/bus-dereference";
import { KeysInitQuery, KeysQuerySourceIdentify, KeysStatistics } from '@comunica/context-entries';
import { KeysRdfJoin, KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, passTestVoid, failTest, IActorArgs } from '@comunica/core';
import { IActionContext, ILink, IQuerySource } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { storeStream } from 'rdf-store-stream';
import { FragmentSelectorShape } from '@comunica/types';
import { MediatorDerivedResourceIdentify } from '@comunica/bus-derived-resource-identify';
import { MediatorDerivedResourceExecute } from '@comunica/bus-derived-resource-execute';
import { MediatorDerivedResourcePartition } from '@comunica/bus-derived-resource-partition';
import { Algebra } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { link } from 'fs';

/**
 * A comunica Solid Derived Resources Extract Links Actor.
 */
export class ActorExtractLinksSolidDerivedResources extends ActorExtractLinks {
  protected readonly derivedResourcePredicates: string[];
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  public readonly mediatorDereference: MediatorDereference;
  public readonly mediatorDerivedResourceIdentify: MediatorDerivedResourceIdentify;
  public readonly mediatorDerivedResourceExecute: MediatorDerivedResourceExecute;
  public readonly mediatorDerivedResourcePartition: MediatorDerivedResourcePartition;
  public readonly queryEngine: QueryEngineBase;

  public constructor(args: IActorExtractLinksSolidDerivedResourcesArgs) {
    super(args);
    this.derivedResourcePredicates = args.derivedResourcePredicates;

    this.mediatorDereferenceRdf = args.mediatorDereferenceRdf;
    this.mediatorDereference = args.mediatorDereference
    this.mediatorDerivedResourceIdentify = args.mediatorDerivedResourceIdentify;
    this.mediatorDerivedResourceExecute = args.mediatorDerivedResourceExecute;
    this.mediatorDerivedResourcePartition = args.mediatorDerivedResourcePartition;

    this.queryEngine = new QueryEngineBase(args.actorInitQuery);
  }

  public async test(action: IActionExtractLinks): Promise<TestResult<IActorTest>> {
    if (!action.context.get(KeysInitQuery.query)) {
      return failTest(`Actor ${this.name} can only work in the context of a query.`);
    }
    if (!action.context.get(KeysQuerySourceIdentify.traverse)) {
      return failTest(`Actor ${this.name} can only work in the context of a traversal query.`);
    }
    return passTestVoid();
  }

  public async run(action: IActionExtractLinks): Promise<IActorExtractLinksOutput> {    
    // TODO BEFORE EXPERIMENT:
    // 1). Deal with overlapping triples when domain also overlaps. 
    // Needs to stateful: Track previous allocations of derived resources
    // so when at later stage we find overlapping resource we know what we did so we dont 'forget' 
    // that we already allocated a part of the sub-query
    // Also when we have all identified derived-resources, we need to do a heuristic to
    // determine the partitioning of queries. For now: prefer-star heuristic.
    // TODO WHEN RUNNING:
    // 1). Formalize some of the things we've implemented here. Think about the temporal aspect
    // when doing overlap / deduplication etc
    // 2). Implement a cost function for determining IF and WHICH derived resources to use.
    // Think about what micro-benchmarks and abalations we need:
    // Abelations:
    // 0. Adaptive only
    // 1. QPF only
    // 2. Triple pattern only
    // 3. Triple pattern / QPF + star
    // 4. Triple pattern / QPF + linear
    // 5. Triple pattern + star + linear (star-only)
    // 6. Triple pattern + star + linear (cost fn)
    // Addition experiments
    // 7. Full system + multiple client scaling
    // Micro-benchmark
    // 8. Show query where using star is bad
    // 9. Show query where data distribution shows star first is bad
    // 10. Show query where we need deduplication 
    // 11. Show query where first choice is bad but adaptive learns later choice to make
    // / where filtering is better / worse than not using / following existing partition
    // TODO AFTER THIS:
    // 1. Possibly make adaptive partitioning strategy that if we find next derived resource
    // we can change our approach
    // 2. Deal with overlapping triples when domain also overlaps. 
    // Needs to stateful: Track previous allocations of derived resources
    // so when at later stage we find overlapping resource we know what we did so we dont 'forget' 
    // that we already allocated a part of the sub-query
    // Also when we have all identified derived-resources, cost-based selection as with 1.
    // 3. After formalizing look at what we still need to implement based on formalizations

    // TODO: How do we deal with potentially overlapping triples in composite resources
    // partitioning the query, estimating cost of triple patterns.
    // how do we filter / execute plan etc

    let context = action.context;
    // Determine links to derived resources
    const derivedResources = [...await this.extractDerivedResourceLinks(action.metadata)];
    if (derivedResources.length === 0) {
      return { links: [] }
    }
    // Set filter immediately to prevent race conditions
    const dynamicLinkFilter = context.getSafe(KeysRdfResolveHypermediaLinks.dynamicFilter);
    derivedResources.forEach(url => {
      dynamicLinkFilter.addExact(url);
    });

    const derivedResourcesRaw: IDerivedResourceRaw[][] = (await Promise.all(derivedResources
      .map(derivedResource => this.dereferenceDerivedResources(derivedResource, action.context))));
    const derivedResourcesUnidentified: IDerivedResourceUnidentified[] = await Promise.all(
      derivedResourcesRaw.flat().map(resource => {
        dynamicLinkFilter.addExact(resource.filterUri.url);
        return this.dereferenceFilter(resource, context);
      }
      ));

    const derivedResourcesIdentifyOutputs = await Promise.all(
      derivedResourcesUnidentified.map(resource =>
        this.mediatorDerivedResourceIdentify.mediate({
          derivedResourceUnidentified: resource,
          context,
        })
          .catch((err) => {
            return null;
          })
      )
    );

    const successfullyIdentified = derivedResourcesIdentifyOutputs.filter(
      result => result !== null
    );

    const derivedResourcesIdentified = successfullyIdentified.map(
      output => output!.derivedResourceIdentified
    );

    // Ensure selected files in derived resource will not be dereferenced again
    derivedResourcesIdentified.forEach((resource) => {
      for (const selector of resource.selectors) {
        dynamicLinkFilter.addGlob(
          selector.replace(/\.[^./*]+$/, '')
        );
      }
    });

    const { resourceExecutionBlocks } = await this.mediatorDerivedResourcePartition.mediate(
      {
        operation: action.context.getSafe(KeysInitQuery.query),
        resources: derivedResourcesIdentified,
        context
      }
    );

    const { links } = await this.mediatorDerivedResourceExecute.mediate({
      executionBlocks: resourceExecutionBlocks,
      context,
    });

    return { links };
  }

  /**
   * Extract links to type index from the metadata stream.
   * @param metadata A metadata quad stream.
   */
  public extractDerivedResourceLinks(metadata: RDF.Stream): Promise<Set<string>> {
    // The metadata stream is shared by all extract links actors, which exceeds the default
    // listener limit, so we raise it like the bus helpers do.
    ActorExtractLinks.allowSharedMetadataListeners(metadata);
    return new Promise<Set<string>>((resolve, reject) => {
      const derivedResourcesInner: Set<string> = new Set();

      // Forward errors
      metadata.on('error', reject);

      metadata.on('data', (quad: RDF.Quad) => {
        if (this.derivedResourcePredicates.includes(quad.predicate.value)) {
          derivedResourcesInner.add(quad.object.value);
        }
      });

      // Resolve to discovered links
      metadata.on('end', () => {
        resolve(derivedResourcesInner);
      });

    });
  }

  public async dereferenceDerivedResources(derivedResource: string, context: IActionContext): Promise<IDerivedResourceRaw[]> {
    // Parse the type index document
    const response = await this.mediatorDereferenceRdf.mediate({ url: derivedResource, context });
    const store = await storeStream(response.data);
    // Query the document to extract all type registrations
    const bindingsArray = await (await this.queryEngine
      .queryBindings(`
        SELECT ?resource ?template ?selector ?filter WHERE {
          ?pod <urn:npm:solid:derived-resources:derivedResource> ?resource .
          ?resource <urn:npm:solid:derived-resources:template> ?template ;
            <urn:npm:solid:derived-resources:selector> ?selector ;
            <urn:npm:solid:derived-resources:filter> ?filter .
        }`, {
        sources: [store],
        [KeysQuerySourceIdentify.traverse.name]: false,
        [KeysRdfJoin.skipAdaptiveJoin.name]: true,
        [KeysStatistics.skipStatisticTracking.name]: true,
        lenient: true,
      })).toArray();

    // Collect derived resources, aggregate selectors belonging to same resource
    const derivedResourcesRaw: Record<string, IDerivedResourceRaw> = {};

    for (const bindings of bindingsArray) {
      const resourceIdentifier = bindings.get('resource')!.value;
      if (!derivedResourcesRaw[resourceIdentifier]) {
        derivedResourcesRaw[resourceIdentifier] = {
          baseUrl: derivedResource.split(".meta")[0],
          template: bindings.get('template')!.value,
          selectors: [],
          filterUri: { url: bindings.get('filter')!.value }
        }
      }
      derivedResourcesRaw[resourceIdentifier].selectors.push(bindings.get('selector')!.value);
    }
    return [...Object.values(derivedResourcesRaw)];
  }

  public async dereferenceFilter(derivedResourcesUnidentified: IDerivedResourceRaw, context: IActionContext):
    Promise<IDerivedResourceUnidentified> {

    const response: IActorDereferenceOutput = await this.mediatorDereference.mediate(
      {
        url: derivedResourcesUnidentified.filterUri.url,
        acceptErrors: true,
        method: "GET",
        // We use the headers to ensure the server knows we are looking for RDF
        headers: new Headers({
          "Accept": "text/turtle,application/n-quads,application/trig,application/ld+json,application/sparql-query"
        }),
        mediaTypes: async () => ({
          // SHACL-based filters use turtle
          "text/turtle": 1.0,
          // Quad pattern indexes are represented as json
          "application/json": 0.4,
          // SPARQL-based filters
          "application/sparql-query": 0.7,
          // QPF uses plain text filter files
          "text/plain": .4,
          // Fallback (TODO: Should we even have this?)
          "*/*": 0.1
        }),
        context,
      }
    );
    const rawText = await this.streamToString(response.data);
    const cleanText = rawText.trim();
    return { ...derivedResourcesUnidentified, filter: cleanText }
  }

  private streamToString(stream: NodeJS.ReadableStream): Promise<string> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
      stream.on('error', (err) => reject(err));
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });
  }

  /**
   * No extraction required
   * @param context 
   * @returns 
   */
  public getExtractPatternRepresentation(context: IActionContext): IExtractPattern[] {
    return []
  }
}


export interface IActorExtractLinksSolidDerivedResourcesArgs
  extends IActorExtractLinksArgs {
  /**
   * The derived resource predicate URLs that will be followed.
   * @default {http://www.w3.org/2007/05/powder-s#describedby}
   */
  derivedResourcePredicates: string[];
  /**
   * An init query actor that is used to query shapes.
   * @default {<urn:comunica:default:init/actors#query>}
   */
  actorInitQuery: ActorInitQueryBase;
  /**
   * The Dereference RDF mediator
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * The dereference mediator for obtaining the filter files
   */
  mediatorDereference: MediatorDereference;
  /**
   * The identify mediator for derived resources
   */
  mediatorDerivedResourceIdentify: MediatorDerivedResourceIdentify;
  /**
   * The execute mediator that runs the work the partition assigned to the derived resources
   */
  mediatorDerivedResourceExecute: MediatorDerivedResourceExecute;
  /**
   * The partition mediator that divides the query over the identified derived resources
   */
  mediatorDerivedResourcePartition: MediatorDerivedResourcePartition;
}

export interface IDerivedResourceRaw {
  /**
   * The base URL where the derived resource is found
   */
  baseUrl: string;
  /**
   * Derived resource template name
   */
  template: string;
  /**
   * The strings representing what data is in the derived resource. This can be glob patterns, specific directories,
   * or files. For directories we represent it as glob pattern. 
   */
  selectors: string[]
  /**
   * URI pointing to the file containing the filter, which produced the data.
   */
  filterUri: ILink
}

export interface IDerivedResourceUnidentified {
  /**
   * The base URL where the derived resource is found
   */
  baseUrl: string;
  /**
   * Derived resource template name
   */
  template: string;
  /**
   * The strings representing what data is in the derived resource. This can be glob patterns, specific directories,
   * or files. For directories we represent it as glob pattern. 
   */
  selectors: string[]
  /**
   * Filter string obtained from derived resource. This will be used in bus-derived-resource-identify to determine
   * the selector shape of the resource and the appropriate actor to use this derived resource.
   */
  filter: string
}

export interface IDerivedResource {
  /**
   * The IRI of the derived resource.
   */
  iri: string;
  /**
   * The base url where this derived resource is found
   */
  baseUrl: string;
  /**
   * Derived resource template name
   */
  template: string;
  /**
   * The strings representing what data is in the derived resource. This can be glob patterns, specific directories,
   * or files. For directories we represent it as glob pattern. 
   */
  selectors: string[]
  /**
   * Filter string obtained from derived resource. This will be used in bus-derived-resource-identify to determine
   * the selector shape of the resource and the appropriate actor to use this derived resource.
   */
  filter: string
  /**
   * The selector shape this derived resource can answer
   */
  derivedResourceSelectorShape: FragmentSelectorShape;
  /**
   * The query source obtained from identifying and dereferencing the derived resource
   */
  querySource: IQuerySource;
  /**
   * Performance coefficients, used to determine the best resource for a given task when multiple resources
   * can be used
   */
  resourceCoefficients: IDerivedResourceCoefficients
}

export interface IDerivedResourceCoefficients {
  /**
   * How costly the resource is server-side
   * e.g. a parameterized query requires full query execution, while precomputed queries or QPF require
   * less compute
   */
  compute: number,
  /**
   * Number of requests required to obtain the full resource. 
   * e.g. a QPF requires more request to obtain the data, while a parameterized single triple pattern
   * query requires only one
   */
  requests: number,
  /**
   * The selectivity of the resource to answer a question
   * e.g. a parameterized join query is highly selective, while a union-based query must be filtered
   */
  selectivity: number
}