import type { ICacheKey, IViewKey } from '@comunica/cache-manager-entries';
import { KeysCaching, KeysQueryOperation } from '@comunica/context-entries';
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
    const iterator = new ClosableTransformIterator(new UnionIterator(
      allIterators,
      { autoStart: false },
    ), {
      autoStart: false,
      onClose: () => {
        firstIterator.close();
        nonAggregatedIterators.close();
      },
    });

    const persistentCacheManager = context.get(KeysCaching.cacheManager);
    // TODO: This should work with any cache view and entries that satisfy the 
    // contract. (As in allows for cardinality estimation?)
    
    // If we 
    // - Have a limit set 
    // - Are passed a cache manager, entry and, view that can count entries 
    // - Are not executing a subquery for bind join
    // then we query the cache for cardinality estimates of the operation.
    if (
       this.setCardinalityFromCacheMinLimit && 
       persistentCacheManager &&
       this.cacheCountViewKey && this.cacheEntryKey &&
       persistentCacheManager.hasCache(this.cacheEntryKey) &&
       persistentCacheManager.hasView(this.cacheCountViewKey) &&
       context.get(KeysQueryOperation.joinBindings) === undefined
      ) {
      firstIterator.getProperty('metadata', async(metadata: MetadataBindings) => {
        const sizeCache = await persistentCacheManager.getRegisteredCache(this.cacheEntryKey!)!.cache.size();
        // We dont update metadata when using fresh cache
        if (sizeCache > this.setCardinalityFromCacheMinLimit!){
          const count = await persistentCacheManager.getFromCache(
            this.cacheEntryKey!,
            this.cacheCountViewKey!,
            { operation },
          );
          if (count) {
            metadata.cardinality = { type: 'estimate', value: count };
          }
          console.log(`Set cardinality: \n ${JSON.stringify(metadata.cardinality, null, 2)}`);
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
