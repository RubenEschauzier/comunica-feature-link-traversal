import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';
import type { MediatorQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import type { MediatorRdfResolveHypermediaLinks } from '@comunica/bus-rdf-resolve-hypermedia-links';
import { KeysHttp, KeysQueryOperation, KeysStatistics } from '@comunica/context-entries';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import type {
  ComunicaDataFactory,
  IActionContext,
  IDiscoverEventData,
  ILink,
  ILinkQueue,
  IQuerySource,
  IStatisticBase,
} from '@comunica/types';
import type { IAggregatedStore, ILinkTraversalManager } from '@comunica/types-link-traversal';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import type { AsyncIterator } from 'asynciterator';
import type { AsyncReiterable } from 'asyncreiterable';
import { AsyncReiterableArray } from 'asyncreiterable';

/**
 * A link traversal manager that traverses over the link queue by resolving query sources for each link, extracting
 * additional links, and appending those to the queue.
 */
export class LinkTraversalManagerMediated implements ILinkTraversalManager {
  protected running = false;
  protected ended = false;
  protected readonly handledUrls: Record<string, boolean> = {};
  /**
   * The documents that only links to descriptions (metadata descriptionOnly) have led to so far, with the
   * links read from them, once read. A description is read for what it describes, such as the services of
   * a dataset, and the links in it are held back, so that following a document to its dataset does not
   * traverse the dataset's container. Once any other link leads to it too, its links are followed.
   */
  protected readonly descriptionsOnly: Record<string, ILink[] | undefined> = {};
  protected linksDereferencing: Set<AbortController> = new Set();
  protected querySourceAggregated: IQuerySource;
  protected querySourcesNonAggregated: AsyncReiterable<IQuerySource>;
  protected rejectionHandler: ((error: Error) => void) | undefined;
  protected readonly stopListeners: (() => void)[] = [];
  private allIteratorsClosedListener: (() => void) | undefined;
  protected linkParallelization: number;

  protected derivedResourcesDereferencing: Set<AbortController> = new Set();

  public constructor(
    protected readonly linkParallelizationDefault: number,
    protected readonly linkParallelizationLimit: number,
    public readonly seeds: ILink[],
    public readonly linkQueue: ILinkQueue,
    public readonly aggregatedStore: IAggregatedStore,
    protected readonly context: IActionContext,
    protected readonly dataFactory: ComunicaDataFactory,
    protected readonly bindingsFactory: BindingsFactory,
    protected readonly mediatorRdfResolveHypermediaLinks: MediatorRdfResolveHypermediaLinks,
    protected readonly mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink,
  ) {
    this.linkParallelization = linkParallelizationDefault;
    this.querySourceAggregated = new QuerySourceRdfJs(
      this.aggregatedStore,
      this.dataFactory,
      this.bindingsFactory,
    );
    this.querySourcesNonAggregated = AsyncReiterableArray.fromInitialEmpty();
  }

  public get started(): boolean {
    return this.running;
  }

  public get stopped(): boolean {
    return this.ended;
  }

  public start(rejectionHandler: (error: Error) => void, context: IActionContext): void {
    if (this.started) {
      throw new Error('Tried to start link traversal manager more than once');
    }

    // Reduce parallelization if we have a LIMIT clause
    // This ensures we do not spam the event loop with traverse events if we need to prioritize query processing events.
    if (context.get(KeysQueryOperation.limitIndicator)) {
      this.linkParallelization = this.linkParallelizationLimit;
    }

    // Prepare link queue iteration
    this.rejectionHandler = rejectionHandler;
    this.running = true;
    for (const link of this.seeds) {
      this.linkQueue.push(link);
      // TODO: This is a universal fix to link traversal
      this.setHandled(link);
    }

    // Stop link queue iteration when all iterators from the aggregated store are closed.
    this.allIteratorsClosedListener = (): void => this.stop();
    this.aggregatedStore.addAllIteratorsClosedListener(this.allIteratorsClosedListener);

    // Kickstart continuous iteration over the link queue
    this.tryTraversingNextLinks();
  }

  public stop(): void {
    if (!this.ended) {
      this.running = false;
      this.ended = true;
      setTimeout(() => this.tryTraversingNextLinks());
    }
  }

  public getQuerySourceAggregated(): IQuerySource {
    return this.querySourceAggregated;
  }

  public getQuerySourcesNonAggregated(): AsyncIterator<IQuerySource> {
    return this.querySourcesNonAggregated.iterator();
  }

  public addStopListener(cb: () => void): void {
    this.stopListeners.push(cb);
  }

  public setHandled(link: ILink) {
    this.handledUrls[link.url] = true;
  }

  protected tryTraversingNextLinks(): void {
    // Stop traversal if needed
    if (!this.running) {
      if (!this.querySourcesNonAggregated.isEnded()) {
        this.querySourcesNonAggregated.push(null);
        this.aggregatedStore.end();
        this.aggregatedStore.removeAllIteratorsClosedListener(this.allIteratorsClosedListener!);
        // If any HTTP requests are still pending, abort them to avoid a hanging Node.js process
        for (const abortController of this.linksDereferencing) {
          abortController.abort();
        }
        for (const abortController of this.derivedResourcesDereferencing) {
          abortController.abort();
        }
        for (const cb of this.stopListeners) {
          cb();
        }
      }
      return;
    }

    // Traverse multiple links in parallel
    while (this.linksDereferencing.size < this.linkParallelization) {
      const nextLink = this.linkQueue.pop();
      if (nextLink) {
        this.followLink(nextLink);
      } else {
        break;
      }
    }

    // If there are no further links to be traversed, we terminate
    if (
      this.linksDereferencing.size === 0 &&
      this.derivedResourcesDereferencing.size === 0 &&
      this.linkQueue.isEmpty()
    ) {
      this.stop();
    }
  }

  protected async getSourceLinks(metadata: Record<string, any>, startLink: ILink): Promise<ILink[]> {
    return this.filterNewLinks(await this.resolveLinks(metadata, startLink));
  }

  /**
   * The links in the metadata of a document, before removing the links followed already.
   */
  protected async resolveLinks(metadata: Record<string, any>, startLink: ILink): Promise<ILink[]> {
    const { links } = await this.mediatorRdfResolveHypermediaLinks.mediate({ context: this.context, metadata });

    // Update discovery event statistic if available
    const traversalTracker: IStatisticBase<IDiscoverEventData> | undefined =
      this.context.get(KeysStatistics.discoveredLinks);
    if (traversalTracker) {
      for (const link of links) {
        traversalTracker.updateStatistic({ url: link.url, metadata: { ...link.metadata }}, startLink);
      }
    }
    return links;
  }

  /**
   * The links not followed yet, to avoid cyclic link loops. A link other than to a description that leads
   * to a document only descriptions led to so far releases the links held back from it.
   */
  protected filterNewLinks(links: ILink[]): ILink[] {
    const released: ILink[] = [];
    const fresh = links.filter((link) => {
      const descriptionOnly = link.metadata?.descriptionOnly === true;
      if (this.handledUrls[link.url]) {
        if (!descriptionOnly && link.url in this.descriptionsOnly) {
          released.push(...this.descriptionsOnly[link.url] ?? []);
          delete this.descriptionsOnly[link.url];
        }
        return false;
      }
      this.setHandled(link);
      if (descriptionOnly) {
        this.descriptionsOnly[link.url] = undefined;
      }
      return true;
    });
    return released.length > 0 ? [ ...fresh, ...this.filterNewLinks(released) ] : fresh;
  }

  protected followLink(nextLink: ILink): void {
    const abortController = new AbortController();
    this.linksDereferencing.add(abortController);

    this.mediatorQuerySourceDereferenceLink.mediate({
      link: nextLink,
      context: this.context
      .set(KeysHttp.httpAbortSignal, abortController.signal)
      .set(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager, this),
    })
      .then(async({ source, metadata }) => {
        // Determine next links, holding them back if only descriptions led here so far
        const links = await this.resolveLinks(metadata, nextLink);
        if (nextLink.url in this.descriptionsOnly) {
          this.descriptionsOnly[nextLink.url] = links;
        } else {
          for (const link of this.filterNewLinks(links)) {
            this.linkQueue.push(link);
          }
        }
        // If the source is a document, add to aggregate store.
        // Otherwise, append to non-document sources.
        if (await source.getFilterFactor(this.context) === 0) {
          this.aggregatedStore.setBaseMetadata(metadata, this.aggregatedStore.containedSources.size > 0);
          await this.aggregatedStore.importSource(nextLink.url, source, this.context);
        } else {
          this.querySourcesNonAggregated.push(source);
        }

        // Queue next iteration
        this.linksDereferencing.delete(abortController);
        setTimeout(() => this.tryTraversingNextLinks());
      })
      .catch((error) => {
        this.linksDereferencing.delete(abortController);
        this.stop();
        this.rejectionHandler!(error);
      });
  }

  public addDereferencingDerivedResource(controller: AbortController): void {
    this.derivedResourcesDereferencing.add(controller);
  }

  public removeDereferencingDerivedResource(controller: AbortController): boolean {
    return this.derivedResourcesDereferencing.delete(controller);
  }

  public completeDereferencingDerivedResource(controller: AbortController, error?: Error): void {
    this.removeDereferencingDerivedResource(controller);
    if (error) {
      this.stop();
      this.rejectionHandler!(error);
    } else {
      setTimeout(() => this.tryTraversingNextLinks());
    }
  }

  public async addLinks(parent: ILink, links: ILink[]): Promise<void> {
    if (!this.running) {
      return;
    }
    for (const link of await this.getSourceLinks({ traverse: links }, parent)) {
      this.linkQueue.push(link);
    }
    setTimeout(() => this.tryTraversingNextLinks());
  }

  public getAggregatedStore(): IAggregatedStore {
    return this.aggregatedStore;
  }
}
