import { CacheEntrySourceState, CacheSourceStateViews, ICacheKey, IViewKey } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries';
import { ActionContext } from '@comunica/core';
import type {
  BindingsStream,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
} from '@comunica/types';
import type { ILinkTraversalManager } from '@comunica/types-link-traversal';
import type { Algebra } from '@comunica/utils-algebra';
import { ClosableTransformIterator } from '@comunica/utils-iterator';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { UnionIterator, wrap as wrapAsyncIterator } from 'asynciterator';
import { MetadataBindings } from '@comunica/types';

/**
 * A query source that operates sources obtained from a link queue.
 */
export class QuerySourceLinkTraversal implements IQuerySource {
  public readonly referenceValue: string;

  public constructor(
    public readonly linkTraversalManager: ILinkTraversalManager,
    protected readonly cacheEntryKey?: ICacheKey<unknown, unknown, unknown>,
    protected readonly cacheViewKey?: IViewKey<unknown, unknown, unknown>,
    // A view over the cache that allows cache queries using quads
    protected readonly cacheCountViewKey?: IViewKey<unknown, {operation: Algebra.Operation; [key: string]: any }, number>,
    protected readonly setCardinalityFromCacheMinLimit?: number,
  ) {
    this.referenceValue = this.linkTraversalManager.seeds.map(link => link.url).join(',');
  }

  public async getSelectorShape(context: IActionContext): Promise<FragmentSelectorShape> {
    return await this.linkTraversalManager.getQuerySourceAggregated().getSelectorShape(context);
  }

  public async getFilterFactor(): Promise<number> {
    return 0;
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    // Start link traversal manager if it had not been started yet
    if (!this.linkTraversalManager.started && !this.linkTraversalManager.stopped) {
      this.linkTraversalManager.start(error => iterator.destroy(error), context);
    }

    // Take the union of the bindings produced when querying over the 
    // aggregated, non-aggregated, and cache sources. We take the metadata of the aggregated source.
    let firstIterator = this.linkTraversalManager.getQuerySourceAggregated()
      .queryBindings(operation, context, options);
    const nonAggregatedIterators = this.linkTraversalManager.getQuerySourcesNonAggregated()
      .map(source => source.queryBindings(operation, context, options));

    let allIterators = nonAggregatedIterators.prepend([ firstIterator ]);

    // TODO: This should be generalized to work with arbitrary cache and view keys that satisfy the
    // contract
    const persistentCacheManager = context.get(KeysCaching.cacheManager);

    let cacheIterator: AsyncIterator<BindingsStream> | undefined = undefined;
    if (persistentCacheManager && this.cacheEntryKey && this.cacheViewKey &&
      persistentCacheManager.hasCache(this.cacheEntryKey) &&
      persistentCacheManager.hasView(this.cacheViewKey)
    ){
      const streamPromise = persistentCacheManager.getFromCache(
          this.cacheEntryKey,
          this.cacheViewKey,
        { operation, mode: 'queryBindings' }
      )

      cacheIterator = wrapAsyncIterator(
        <Promise<AsyncIterator<BindingsStream>>> streamPromise, { autoStart: false}
      );

      const stopIterator = async () => {
        await persistentCacheManager.getFromCache(
          this.cacheEntryKey!,
          this.cacheViewKey!,
          { url: "end", mode: 'get', action: { link: { url: 'end' }, context: new ActionContext() } }
        )
      }
      this.linkTraversalManager.addStopListener(() => stopIterator());
      allIterators = allIterators.prepend(cacheIterator);
    }

    const iterator = new ClosableTransformIterator(new UnionIterator(
      allIterators,
      { autoStart: false }), {
      autoStart: false,
      onClose: () => {
        firstIterator.close();
        nonAggregatedIterators.close();
        cacheIterator?.close()
      },
    });
    if (this.setCardinalityFromCacheMinLimit){
      firstIterator.getProperty('metadata', (metadata: MetadataBindings) => {
        if (persistentCacheManager && this.cacheCountViewKey && this.cacheEntryKey &&
          persistentCacheManager.hasCache(this.cacheEntryKey) &&
          persistentCacheManager.hasView(this.cacheCountViewKey)
        ){
          const metadataPromise = (async () => {
            try {
              // Ensure the cache is sufficiently full before using it to estimate cardinality
              const sizeCache = await persistentCacheManager.getRegisteredCache(this.cacheEntryKey!)!.cache.size();
              if (sizeCache > this.setCardinalityFromCacheMinLimit!) return metadata;
              
              // Query cache for cardinalities
              const count = await persistentCacheManager.getFromCache(
                this.cacheEntryKey!,
                this.cacheCountViewKey!, 
                { operation }
              );

              if (count) {
                metadata.cardinality = { type: 'estimate', value: count };
                console.log(`Cardinality from cache: ${count}`);
              }
            } catch (error) {
              console.error('Failed to retrieve cache operation count:', error);
            }

            return metadata;
          })();
          console.log("SEtting")
          iterator.setProperty('metadata', metadataPromise)
        }
        else{
          iterator.setProperty('metadata', metadata)
        }
      });
    }
    else{
      firstIterator.getProperty('metadata', metadata => iterator.setProperty('metadata', metadata));
    }
    return iterator;
  }

  public queryQuads(): AsyncIterator<RDF.Quad> {
    throw new Error('queryQuads is not implemented in QuerySourceLinkTraversal');
  }

  public queryBoolean(): Promise<boolean> {
    throw new Error('queryBoolean is not implemented in QuerySourceLinkTraversal');
  }

  public queryVoid(): Promise<void> {
    throw new Error('queryVoid is not implemented in QuerySourceLinkTraversal');
  }

  public toString(): string {
    return `QuerySourceLinkTraversal(${this.referenceValue})`;
  }
}
