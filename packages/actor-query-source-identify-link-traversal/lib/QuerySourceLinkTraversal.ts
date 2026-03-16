import type { ICacheKey, IViewKey } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries';
import type {
  BindingsStream,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
  MetadataBindings,
} from '@comunica/types';
import type { ILinkTraversalManager } from '@comunica/types-link-traversal';
import type { Algebra } from '@comunica/utils-algebra';
import { ClosableTransformIterator } from '@comunica/utils-iterator';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { UnionIterator, wrap as wrapAsyncIterator } from 'asynciterator';

/**
 * A query source that operates sources obtained from a link queue.
 */
export class QuerySourceLinkTraversal implements IQuerySource {
  public readonly referenceValue: string;
  public callToThis = 0;

  public constructor(
    public readonly linkTraversalManager: ILinkTraversalManager,
    protected readonly cacheEntryKey?: ICacheKey<unknown, unknown, unknown>,
    protected readonly cacheViewKey?: IViewKey<unknown, unknown, unknown>,
    // A view over the cache that allows cache queries using quads
    protected readonly cacheCountViewKey?: IViewKey<unknown, { operation: Algebra.Operation; [key: string]: any }, number>,
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
    // aggregated and non-aggregated. We take the metadata of the aggregated source.
    const firstIterator = this.linkTraversalManager.getQuerySourceAggregated()
      .queryBindings(operation, context, options);
    const nonAggregatedIterators = this.linkTraversalManager.getQuerySourcesNonAggregated()
      .map(source => source.queryBindings(operation, context, options));

    let allIterators = nonAggregatedIterators.prepend([ firstIterator ]);
    // TODO: This should be generalized to work with arbitrary cache and view keys that satisfy the
    // contract
    const persistentCacheManager = context.get(KeysCaching.cacheManager);

    // let cacheIterator: BindingsStream | undefined;
    // if (persistentCacheManager && this.cacheEntryKey && this.cacheViewKey &&
    //   persistentCacheManager.hasCache(this.cacheEntryKey) &&
    //   persistentCacheManager.hasView(this.cacheViewKey)
    // ) {
    //   const streamPromise = persistentCacheManager.getFromCache(
    //     this.cacheEntryKey,
    //     this.cacheViewKey,
    //     { operation, mode: 'queryBindings', context },
    //   );

    //   cacheIterator = wrapAsyncIterator(
    //     <Promise<BindingsStream>> streamPromise,
    //     { autoStart: false },
    //   );

    //   const stopIterator = async() => {
    //     await persistentCacheManager.getFromCache(
    //       this.cacheEntryKey!,
    //       this.cacheViewKey!,
    //       { url: 'end', mode: 'get', action: { link: { url: 'end' }, context }},
    //     );
    //   };
    //   this.linkTraversalManager.addStopListener(() => stopIterator());
    //   allIterators = allIterators.prepend([ cacheIterator ]);
    // }

    const iterator = new ClosableTransformIterator(new UnionIterator(
      allIterators,
      { autoStart: false },
    ), {
      autoStart: false,
      onClose: () => {
        firstIterator.close();
        nonAggregatedIterators.close();
        // cacheIterator?.close();
      },
    });

    // If we have the limit set and are passed a cache view that can count entries in the cache
    // query the cache for cardinality estimates of the operation.
    if (this.setCardinalityFromCacheMinLimit && persistentCacheManager &&
       this.cacheCountViewKey && this.cacheEntryKey &&
          persistentCacheManager.hasCache(this.cacheEntryKey) &&
          persistentCacheManager.hasView(this.cacheCountViewKey)) {
      firstIterator.getProperty('metadata', async(metadata: MetadataBindings) => {
        const sizeCache = await persistentCacheManager.getRegisteredCache(this.cacheEntryKey!)!.cache.size();
        if (sizeCache > this.setCardinalityFromCacheMinLimit!) {
          // Query cache for cardinalities
          const count = await persistentCacheManager.getFromCache(
            this.cacheEntryKey!,
            this.cacheCountViewKey!,
            { operation },
          );

          if (count) {
            metadata.cardinality = { type: 'estimate', value: count };
            console.log(`Cardinality from cache: ${count}`);
          }
        }
        iterator.setProperty('metadata', metadata);
      });
    } else {
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
