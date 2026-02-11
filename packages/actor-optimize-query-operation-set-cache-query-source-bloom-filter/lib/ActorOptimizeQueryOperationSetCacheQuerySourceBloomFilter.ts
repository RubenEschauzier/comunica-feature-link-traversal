import { CacheEntrySourceState, CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContext, passTestVoid } from '@comunica/core';
import type { BindingsStream, ISourceState, ISourceStateBloomFilter } from '@comunica/types';

import type { ICacheView, IPersistentCache, ISetFn } from '@comunica/types';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { PersistentCacheSourceStateIndexedBloomFilter } from './PersistentCacheSourceStateIndexedBloomFilter';
import { AsyncIterator, EmptyIterator} from 'asynciterator';
import type * as RDF from '@rdfjs/types';
import { IActionQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { AsyncReiterableArray } from 'asyncreiterable';
import { KeysQuerySourceIdentify } from '@comunica/context-entries';
import { 
  ActorOptimizeQueryOperation, 
  IActionOptimizeQueryOperation, 
  IActorOptimizeQueryOperationArgs, 
  IActorOptimizeQueryOperationOutput 
} from '@comunica/bus-optimize-query-operation';

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheQuerySourceBloomFilter extends ActorOptimizeQueryOperation {
  private readonly cacheQuerySourceState: PersistentCacheSourceStateIndexedBloomFilter;

  public constructor(args: IActorOptimizeQueryOperationSetCacheQuerySourceBloomFilterArgs) {
    super(args);
    this.cacheQuerySourceState = new PersistentCacheSourceStateIndexedBloomFilter(
      { maxNumTriples: args.cacheSizeNumTriples },
    );
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    return passTestVoid(); 
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    const context = action.context;
    if (!action.context.get(KeysQuerySourceIdentify.traverse)){
      return { context, operation: action.operation };
    }

    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceStateQuerySourceBloomFilter,
      this.cacheQuerySourceState,
      new SetSourceStateCache(),
    );

    cacheManager.registerCacheView(
      CacheSourceStateViews.cacheQueryViewBloomFilter,
      new GetSourceStateCacheViewBloomFilter(),
    );
    
    return { context, operation: action.operation };

  }
}


export class SetSourceStateCache implements ISetFn<ISourceStateBloomFilter, ISourceStateBloomFilter, { headers: Headers }> {
  protected DF: DataFactory = new DataFactory();
  protected AF: AlgebraFactory = new AlgebraFactory(this.DF);

  public async setInCache(
    key: string,
    value: ISourceStateBloomFilter,
    cache: IPersistentCache<ISourceStateBloomFilter>,
    context: { headers: Headers },
  ): Promise<void> {
    cache.set(key, value);
  }
}

export class GetSourceStateCacheViewBloomFilter
implements ICacheView<
  ISourceStateBloomFilter, 
  { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink } | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
  AsyncIterator<BindingsStream> | AsyncIterator<AsyncIterator<RDF.Quad>> | ISourceStateBloomFilter
> {
  protected querySourcesCached: AsyncReiterableArray<ISourceStateBloomFilter> = AsyncReiterableArray.fromInitialEmpty();
  protected ended = false;
  protected pendingCount = 0;

  public constructor(){
  }

  protected checkForTermination() {
    // Only close if we have received the 'end' signal AND there are no active lookups
    if (this.ended && this.pendingCount === 0) {
      this.querySourcesCached.push(null);
    }
  }

  /**
   * Checks if a source should be queried based on the operation and the source's Bloom Filter.
   */
  protected shouldQuerySource(source: ISourceStateBloomFilter, operation: Algebra.Pattern): boolean {
    if (!source.bloomFilter) {
      return true;
    }
    if (operation.subject.termType !== 'Variable' && !source.bloomFilter.has(operation.subject.value)) {
      return false;
    }
    if (operation.object.termType !== 'Variable' && !source.bloomFilter.has(operation.object.value)) {
      return false;
    }
    return true;
  }

  public async construct(
    cache: IPersistentCache<ISourceStateBloomFilter>,
    context: { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink} | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
  ): Promise<AsyncIterator<BindingsStream> | AsyncIterator<AsyncIterator<RDF.Quad>>  | ISourceStateBloomFilter | undefined> {
    if (context.mode === 'get'){
      if (context.url === 'end'){
        if (!this.ended) {
          this.ended = true;
          this.checkForTermination();
        }
        return;
      }
      this.pendingCount++;
      try {
        const cacheEntry = await cache.get(context.url);
        
        // Only push if valid and policy satisfied
        if (cacheEntry && cacheEntry.cachePolicy?.satisfiesWithoutRevalidation(context.action)){
          this.querySourcesCached.push(cacheEntry);
          return cacheEntry;
        }
      } finally {
        this.pendingCount--;
        this.checkForTermination();
      }
    }
    else if (context.mode === 'queryBindings'){
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        const iteratorSources = this.querySourcesCached.iterator();
        return iteratorSources.map(source => {
          if (this.shouldQuerySource(source, <Algebra.Pattern> context.operation)){
            return source.source.queryBindings(context.operation, new ActionContext())
          }
          return new EmptyIterator<RDF.Bindings>();
        })
      }
      else {
        throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
      }
    }
    else if (context.mode === 'queryQuads'){
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        const pattern = context.operation as Algebra.Pattern;

        // Filter sources based on Bloom Filter before creating quad streams
        const iteratorSources = this.querySourcesCached.iterator()
          .filter(source => this.shouldQuerySource(source, pattern));

        return iteratorSources.map(source => source.source.queryQuads(context.operation, new ActionContext()));
      }
      else {
        throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
      }
    }
    else {
      throw new Error(`Unknown view mode passed to ${this.constructor.name}: ${context.mode}`)
    }
  }
}


export interface IActorOptimizeQueryOperationSetCacheQuerySourceBloomFilterArgs extends IActorOptimizeQueryOperationArgs {
    /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124000}
   */
  cacheSizeNumTriples: number;
}

