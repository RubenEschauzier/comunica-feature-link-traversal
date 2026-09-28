import { ActorInitQueryBase, QueryEngineBase } from '@comunica/actor-init-query';
import { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import { ActorExtractLinks, IActionExtractLinks, IActorExtractLinksOutput, IActorExtractLinksArgs, IExtractPattern } from '@comunica/bus-extract-links';
import { MediatorDereference } from "@comunica/bus-dereference";
import { KeysInitQuery, KeysQuerySourceIdentify, KeysStatistics } from '@comunica/context-entries';
import { KeysQuerySourceIdentifyLinkTraversal, KeysRdfJoin, KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, passTestVoid, failTest, IActorArgs } from '@comunica/core';
import { IActionContext, ILink, IQuerySource } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { FragmentSelectorShape } from '@comunica/types';
import { MediatorDerivedResourceIdentify } from '@comunica/bus-derived-resource-identify';
import { MediatorDerivedResourceExecute } from '@comunica/bus-derived-resource-execute';
import { MediatorDerivedResourcePartition } from '@comunica/bus-derived-resource-partition';
import { Algebra } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { DerivedResourceDescriptionFetcher } from './DerivedResourceDescriptionFetcher';
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
  protected readonly descriptionFetcher: DerivedResourceDescriptionFetcher;

  public constructor(args: IActorExtractLinksSolidDerivedResourcesArgs) {
    super(args);
    this.derivedResourcePredicates = args.derivedResourcePredicates;

    this.mediatorDereferenceRdf = args.mediatorDereferenceRdf;
    this.mediatorDereference = args.mediatorDereference
    this.mediatorDerivedResourceIdentify = args.mediatorDerivedResourceIdentify;
    this.mediatorDerivedResourceExecute = args.mediatorDerivedResourceExecute;
    this.mediatorDerivedResourcePartition = args.mediatorDerivedResourcePartition;

    this.queryEngine = new QueryEngineBase(args.actorInitQuery);
    this.descriptionFetcher = new DerivedResourceDescriptionFetcher(this.mediatorDereferenceRdf, this.mediatorDereference);
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
    const dynamicLinkFilter = context.getSafe(KeysRdfResolveHypermediaLinks.dynamicFilter);
    // Determine links to derived resources. Every document of a pod can point at the same
    // description, and once the pod is crawled rather than covered those documents are read too; a
    // description the filter already holds was handled for an earlier one, and handling it again
    // would hand the join a second copy of every composite answer
    const derivedResources = [...await this.extractDerivedResourceLinks(action.metadata)]
      .filter(url => !dynamicLinkFilter.matchesFilter(url));
    if (derivedResources.length === 0) {
      return { links: [] }
    }
    // Set filter immediately to prevent race conditions
    derivedResources.forEach(url => {
      dynamicLinkFilter.addExact(url);
    });

    // The rest is left to run in the background. Traversal only imports a document once its links
    // are known, so waiting here would keep the document the derived resources were found in, often
    // the seed, out of the store until every derived resource is identified and executed. The
    // manager is told the work is pending, so traversal does not end before its links are pushed.
    const manager = context.get(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager);
    if (!manager) {
      return { links: await this.executeDerivedResources(action, derivedResources, () => {}) };
    }

    // The links just returned can lead into the pods these resources cover, but which documents they
    // cover is only known once the resources are executed. Until then those links are held back
    // rather than crawled, as a crawled document would be answered a second time by its resource
    const releases = derivedResources.map(url => dynamicLinkFilter.hold(this.podBaseUrl(url)));
    const release = (): void => releases.forEach(releaseHold => releaseHold());

    const controller = new AbortController();
    manager.addDereferencingDerivedResource(controller);
    this.executeDerivedResources(action, derivedResources, release)
      .then(async(links) => {
        if (!controller.signal.aborted) {
          await manager.addLinks({ url: action.url }, links);
        }
      })
      // A failing derived resource did not fail the query while the mediator filtered its errors,
      // and must not now that it runs outside of it
      .catch(() => {})
      .finally(() => {
        release();
        manager.completeDereferencingDerivedResource(controller);
      });

    return { links: [] };
  }

  /**
   * Identifies, partitions and executes the derived resources, returning the links they lead to.
   */
  protected async executeDerivedResources(
    action: IActionExtractLinks,
    derivedResources: string[],
    releaseHeldLinks: () => void,
  ): Promise<ILink[]> {
    const context = action.context;
    const dynamicLinkFilter = context.getSafe(KeysRdfResolveHypermediaLinks.dynamicFilter);

    // Often already underway, as the descriptions of the seeds are fetched before the seeds are
    const derivedResourcesUnidentified: IDerivedResourceUnidentified[] = (await Promise.all(derivedResources
      .map(derivedResource => this.descriptionFetcher.get(derivedResource, context)))).flat();
    for (const resource of derivedResourcesUnidentified) {
      if (resource.filterUri) {
        dynamicLinkFilter.addExact(resource.filterUri.url);
      }
    }

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

    // Which documents no longer have to be crawled is left to the execute actors: only one that
    // answers every pattern over a resource's documents, as the triple pattern actor does, may take
    // them off the crawl. A composite answers just its own sub-query, so the rest of the query still
    // needs those documents

    const { resourceExecutionBlocks } = await this.mediatorDerivedResourcePartition.mediate(
      {
        operation: action.context.getSafe(KeysInitQuery.query),
        resources: derivedResourcesIdentified,
        documentUrl: action.url,
        context
      }
    );

    const { links } = await this.mediatorDerivedResourceExecute.mediate({
      executionBlocks: resourceExecutionBlocks,
      context,
    });
    // The execute actors have now told the filter what they cover, so links into these pods can be
    // decided on
    releaseHeldLinks();

    return links;
  }

  /**
   * The url of the pod a derived resource description belongs to.
   */
  protected podBaseUrl(derivedResource: string): string {
    return derivedResource.split('.meta')[0];
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
  /**
   * URI of the file the filter was read from.
   */
  filterUri?: ILink
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