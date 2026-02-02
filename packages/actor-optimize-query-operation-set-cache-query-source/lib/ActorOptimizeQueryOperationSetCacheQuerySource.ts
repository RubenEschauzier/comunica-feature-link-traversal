import type {
  IActionContextPreprocess,
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { CacheEntrySourceState, CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContext, ActionContextKey, failTest, passTestVoid } from '@comunica/core';
import type { Bindings, BindingsStream, ISourceState } from '@comunica/types';

import type { ICacheView, IPersistentCache, ISetFn } from '@comunica/types-link-traversal';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { PersistentCacheSourceStateIndexed } from './PersistentCacheSourceStateIndexed';
import { BufferedIterator, AsyncIterator} from 'asynciterator';
import type * as RDF from '@rdfjs/types';
import { IActionQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { AsyncReiterableArray } from 'asyncreiterable';
import { KeysQuerySourceIdentify } from '@comunica/context-entries';
import { ActorOptimizeQueryOperation, IActionOptimizeQueryOperation, IActorOptimizeQueryOperationArgs, IActorOptimizeQueryOperationOutput } from '@comunica/bus-optimize-query-operation';
/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheQuerySource extends ActorOptimizeQueryOperation {
  private readonly cacheQuerySourceState: PersistentCacheSourceStateIndexed;

  public constructor(args: IActorOptimizeQueryOperationSetCacheQuerySourceArgs) {
    super(args);
    this.cacheQuerySourceState = new PersistentCacheSourceStateIndexed(
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
      CacheEntrySourceState.cacheSourceStateQuerySource,
      this.cacheQuerySourceState,
      new SetSourceStateCache(),
    );

    cacheManager.registerCacheView(
      CacheSourceStateViews.cacheQueryView,
      new GetSourceStateCacheView(),
    );
    
    return { context, operation: action.operation };

  }
}


export class SetSourceStateCache implements ISetFn<ISourceState, ISourceState, { headers: Headers }> {
  protected DF: DataFactory = new DataFactory();
  protected AF: AlgebraFactory = new AlgebraFactory(this.DF);

  public async setInCache(
    key: string,
    value: ISourceState,
    cache: IPersistentCache<ISourceState>,
    context: { headers: Headers },
  ): Promise<void> {
    cache.set(key, value);
  }
}

export class GetSourceStateCacheView
implements ICacheView<
  ISourceState, 
  { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink } | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
  AsyncIterator<BindingsStream> | AsyncIterator<AsyncIterator<RDF.Quad>> | ISourceState
> {
  protected querySourcesCached: AsyncReiterableArray<ISourceState> = AsyncReiterableArray.fromInitialEmpty();
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


  public async construct(
    cache: IPersistentCache<ISourceState>,
    context: { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink} | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
  ): Promise<AsyncIterator<BindingsStream> | AsyncIterator<AsyncIterator<RDF.Quad>>  | ISourceState | undefined> {
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
        return iteratorSources.map(source => source.source.queryBindings(context.operation, new ActionContext()))
      }
      else {
        throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
      }
    }
    else if (context.mode === 'queryQuads'){
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        const iteratorSources = this.querySourcesCached.iterator();
        return iteratorSources.map(source => source.source.queryQuads(context.operation, new ActionContext()))
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



export interface IActorOptimizeQueryOperationSetCacheQuerySourceArgs extends IActorOptimizeQueryOperationArgs {
    /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124000}
   */
  cacheSizeNumTriples: number;
}

