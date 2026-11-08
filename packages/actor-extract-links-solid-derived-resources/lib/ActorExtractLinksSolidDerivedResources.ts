import { ActorInitQueryBase, QueryEngineBase } from '@comunica/actor-init-query';
import { ActorExtractLinks, IActionExtractLinks, IActorExtractLinksOutput, IActorExtractLinksArgs, IExtractPattern } from '@comunica/bus-extract-links';
import { KeysInitQuery, KeysQuerySourceIdentify, KeysStatistics } from '@comunica/context-entries';
import { KeysQuerySourceIdentifyLinkTraversal, KeysRdfJoin, KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, passTestVoid, failTest, IActorArgs } from '@comunica/core';
import { IActionContext, ILink, IQuerySource } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { FragmentSelectorShape } from '@comunica/types';
import { MediatorDerivedResourceIdentify } from '@comunica/bus-derived-resource-identify';
import { CompositeResponseGate, MediatorDerivedResourceExecute } from '@comunica/bus-derived-resource-execute';
import { MediatorDerivedResourcePartition } from '@comunica/bus-derived-resource-partition';
import { Algebra } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import type { IDiscoveredResources, MediatorDerivedResourceDiscover } from '@comunica/bus-derived-resource-discover';
import type { IDynamicFilter } from '@comunica/types-link-traversal';
import { Readable } from 'readable-stream';

/**
 * The resources taken up so far, per query. Several documents and descriptions can link the same
 * resource, and every time it is taken up it answers its part of the query again. Keyed by the
 * dynamic link filter, which lives exactly as long as the query. Kept outside of the actor, as the
 * components generator cannot read the type of a field holding it.
 */
const claimedResources = new WeakMap<IDynamicFilter, Set<string>>();

/**
 * A comunica Solid Derived Resources Extract Links Actor.
 *
 * Takes up the resources with query capability a document links to, which are found on the
 * derived-resource-discover bus, and has them identified, partitioned and executed by the derived
 * resource actors.
 */
export class ActorExtractLinksSolidDerivedResources extends ActorExtractLinks {
  public readonly mediatorDerivedResourceDiscover: MediatorDerivedResourceDiscover;
  public readonly mediatorDerivedResourceIdentify: MediatorDerivedResourceIdentify;
  public readonly mediatorDerivedResourceExecute: MediatorDerivedResourceExecute;
  public readonly mediatorDerivedResourcePartition: MediatorDerivedResourcePartition;
  public readonly holdPodLinks: boolean;
  public readonly queryEngine: QueryEngineBase;

  public constructor(args: IActorExtractLinksSolidDerivedResourcesArgs) {
    super(args);
    this.mediatorDerivedResourceDiscover = args.mediatorDerivedResourceDiscover;
    this.mediatorDerivedResourceIdentify = args.mediatorDerivedResourceIdentify;
    this.mediatorDerivedResourceExecute = args.mediatorDerivedResourceExecute;
    this.mediatorDerivedResourcePartition = args.mediatorDerivedResourcePartition;
    this.holdPodLinks = args.holdPodLinks;

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
    if (this.isInterface(context, action.url)) {
      return { links: [] };
    }
    // Find the resources the document links to. A description that every document of a pod links to
    // is taken up by the first of them only, by the actor following the link, as taking it up again
    // would hand the join a second copy of every composite answer
    const { discovered } = await this.mediatorDerivedResourceDiscover.mediate({
      url: action.url,
      metadata: this.forwardMetadata(action.metadata),
      describedUrl: action.url,
      kind: 'document',
      context,
    });
    
    if (discovered.length === 0) {
      return { links: [] }
    }

    // The links extracted from this actor are returned async (if a manager exists),
    // to prevent the relatively large amount of work done in this actor to stall traversal.
    const manager = context.get(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager);
    if (!manager) {
      return { links: await this.executeDerivedResources(action, discovered, () => {}) };
    }

    // The links just returned can lead into the pods these resources cover, but which documents they
    // cover is only known once the resources are executed. Until then those links are held back
    // rather than crawled, as a crawled document would be answered a second time by its resource
    // Without holding, those links are crawled as usual, see holdPodLinks
    const releases = this.holdPodLinks ?
      discovered.map(({ scope }) => dynamicLinkFilter.hold(scope)) :
      [];
    const release = (): void => releases.forEach(releaseHold => releaseHold());

    const controller = new AbortController();
    manager.addDereferencingDerivedResource(controller);
    this.executeDerivedResources(action, discovered, release)
      .then(async(links) => {
        // Here discovered links are added back to the manager
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
   * @param action The extract links action.
   * @param discovered The resources the document links to, as discovered.
   * @param releaseHeldLinks Releases the links held back until it is known what is covered.
   */
  protected async executeDerivedResources(
    action: IActionExtractLinks,
    discovered: IDiscoveredResources[],
    releaseHeldLinks: () => void,
  ): Promise<ILink[]> {
    const context = action.context;
    const dynamicLinkFilter = context.getSafe(KeysRdfResolveHypermediaLinks.dynamicFilter);

    // Often already underway, as the descriptions of the seeds are fetched before the seeds are. A
    // description that cannot be read, such as one that does not exist, leaves the others usable
    const found = await Promise.all(discovered.map(({ resources }) => resources.catch(() => [])));
    // Several documents and descriptions can link the same resource, such as the SPARQL endpoint of
    // the pod, which is taken up once
    const derivedResourcesUnidentified = this.claimResources(context, found.flat());
    if (derivedResourcesUnidentified.length === 0) {
      releaseHeldLinks();
      return [];
    }
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

    // A composite answers over the union of the documents it aggregates, so it can only join over them
    // if its server gives every blank node one label throughout them, in the documents as well as in
    // the answers. Publishing a composite thus implies that, and the blank nodes read from its
    // container are given one identity per label from here on. Done before anything is executed, so
    // the answers of the resources already share the blank nodes of the documents read so far.
    // An interface implies no such thing: a SPARQL endpoint labels blank nodes on its own terms
    const store = context.get(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager)?.getAggregatedStore();
    const scoped = derivedResourcesIdentified.filter(resource => !resource.interfaceType);
    for (const baseUrl of new Set(scoped.map(resource => resource.baseUrl))) {
      store?.addBlankNodeScope?.(baseUrl);
    }

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

    // Without composite blocks there are no composite answers to wait for
    const compositeResponses = new CompositeResponseGate();
    if (!resourceExecutionBlocks.some(block => block.type === 'composite')) {
      compositeResponses.close();
    }
    const { links } = await this.mediatorDerivedResourceExecute.mediate({
      executionBlocks: resourceExecutionBlocks,
      compositeResponses,
      context,
    });
    // The execute actors have now told the filter what they cover, so links into these pods can be
    // decided on
    releaseHeldLinks();

    return links;
  }

  /**
   * The resources not yet taken up in this query, which are claimed for it. A resource is known by
   * the url its template resolves to. Those urls are kept off the crawl as well: they answer queries
   * rather than describe data.
   */
  protected claimResources<T extends IDerivedResourceRaw>(context: IActionContext, resources: T[]): T[] {
    const dynamicLinkFilter = context.getSafe(KeysRdfResolveHypermediaLinks.dynamicFilter);
    let claimed = claimedResources.get(dynamicLinkFilter);
    if (!claimed) {
      claimed = new Set();
      claimedResources.set(dynamicLinkFilter, claimed);
    }
    const unclaimed: T[] = [];
    for (const resource of resources) {
      const url = this.resourceUrl(resource);
      if (!claimed.has(url)) {
        claimed.add(url);
        dynamicLinkFilter.addExact(url);
        unclaimed.push(resource);
      }
    }
    return unclaimed;
  }

  /**
   * Whether the url is that of an interface rather than of a document of a pod: a query source given
   * with a type, such as a SPARQL endpoint, or a resource taken up in this query. What links are read from
   * those is read from answers and descriptions of the interface, which link no derived resources and lie
   * in no container of a pod.
   */
  protected isInterface(context: IActionContext, url: string): boolean {
    const dynamicLinkFilter = context.getSafe(KeysRdfResolveHypermediaLinks.dynamicFilter);
    if (claimedResources.get(dynamicLinkFilter)?.has(url)) {
      return true;
    }
    const sources: any[] = context.get(KeysInitQuery.querySourcesUnidentified) ?? [];
    return sources.some(source => typeof source === 'object' && source !== null &&
      source.type !== undefined && (source.value?.value ?? source.value) === url);
  }

  /**
   * The url a resource is found at, which is its template resolved against its base url.
   */
  protected resourceUrl(resource: IDerivedResourceRaw): string {
    try {
      return new URL(resource.template, resource.baseUrl).href;
    } catch {
      return resource.baseUrl + resource.template;
    }
  }

  /**
   * A stream of the quads of the metadata stream, for the discover actors to read.
   *
   * The metadata stream is shared by all extract links actors, and flows as soon as one of them reads
   * it. The discover actors attach to what they are given only once the mediator has awaited all of
   * their tests, and should one of those wait on the event loop, the stream has passed them by, ended
   * included, so the discovery would never end. They are thus given a stream of their own, attached to
   * now, that keeps what comes in until they read it.
   * @param metadata A metadata quad stream.
   */
  protected forwardMetadata(metadata: RDF.Stream): RDF.Stream {
    // The metadata stream is shared by all extract links actors, which exceeds the default
    // listener limit, so we raise it like the bus helpers do.
    ActorExtractLinks.allowSharedMetadataListeners(metadata);
    const forwarded = new Readable({ objectMode: true, read: () => {} });
    metadata.on('data', (quad: RDF.Quad) => forwarded.push(quad));
    metadata.on('end', () => forwarded.push(null));
    metadata.on('error', (error: Error) => forwarded.destroy(error));
    return <RDF.Stream> <unknown> forwarded;
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
   * The discover mediator finding the resources a document links to.
   */
  mediatorDerivedResourceDiscover: MediatorDerivedResourceDiscover;
  /**
   * An init query actor that is used to query shapes.
   * @default {<urn:comunica:default:init/actors#query>}
   */
  actorInitQuery: ActorInitQueryBase;
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
  /**
   * Whether links into a pod are held back while its derived resources are identified and executed.
   *
   * A resource that answers every pattern over the pod's documents takes those documents off the crawl
   * once it has been executed; holding keeps them from being crawled before then, which would fetch and
   * import the same data twice. Where no resource does that, as with composite resources only, nothing
   * is ever taken off the crawl and holding only delays it, so it can be disabled.
   * @default {true}
   */
  holdPodLinks: boolean;
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
   * Absent when the description holds the filter itself.
   */
  filterUri?: ILink
  /**
   * The filter itself, when the description holds it rather than pointing to it.
   */
  filter?: string
  /**
   * The predicate an interface was linked with, which says what kind of interface it is, such as
   * `void:sparqlEndpoint`. Absent for derived resources, which are known by their template and filter.
   */
  interfaceType?: string;
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
  /**
   * The predicate an interface was linked with, which says what kind of interface it is, such as
   * `void:sparqlEndpoint`. Absent for derived resources, which are known by their template and filter.
   */
  interfaceType?: string;
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
  /**
   * The predicate an interface was linked with, which says what kind of interface it is, such as
   * `void:sparqlEndpoint`. Absent for derived resources, which are known by their template and filter.
   */
  interfaceType?: string;
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