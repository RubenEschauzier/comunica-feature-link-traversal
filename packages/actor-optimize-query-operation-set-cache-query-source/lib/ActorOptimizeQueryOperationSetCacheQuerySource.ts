import { CacheEntrySourceState, CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching, KeysInitQuery, KeysQueryOperation } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContext, ActionContextKey, passTestVoid } from '@comunica/core';
import type {  BindingsStream, IActionContext, IQuerySource, ISourceState } from '@comunica/types';

import type { ICacheView, IPersistentCache, ISetFn } from '@comunica/types';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { PersistentCacheSourceStateIndexed } from './PersistentCacheSourceStateIndexed';
import { AsyncIterator, wrap as wrapAsyncIterator, ArrayIterator } from 'asynciterator';
import type * as RDF from '@rdfjs/types';
import { IActionQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { KeysQuerySourceIdentify } from '@comunica/context-entries';
import { ActorOptimizeQueryOperation, IActionOptimizeQueryOperation, IActorOptimizeQueryOperationArgs, IActorOptimizeQueryOperationOutput } from '@comunica/bus-optimize-query-operation';
import {  StreamingStore } from 'rdf-streaming-store';
import { quadsToBindings } from '@comunica/bus-query-source-identify';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { visitOperation } from '@comunica/utils-algebra/lib/utils';
import { ClosableTransformIterator } from '@comunica/utils-iterator';
import { MediatorFactoryAggregatedStore } from '@comunica/bus-factory-aggregated-store';
import { IAggregatedStore } from '@comunica/types-link-traversal';
import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheQuerySource extends ActorOptimizeQueryOperation {
  private readonly cacheSizeNumTriples: number;
  private cacheQuerySourceState: PersistentCacheSourceStateIndexed;
  
  public readonly mediatorFactoryAggregatedStore: MediatorFactoryAggregatedStore;

  public constructor(args: IActorOptimizeQueryOperationSetCacheQuerySourceArgs) {
    super(args);
    this.cacheSizeNumTriples = args.cacheSizeNumTriples;
    this.mediatorFactoryAggregatedStore = args.mediatorFactoryAggregatedStore;

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
    
    if (context.get(KeysCaching.clearCache) || context.get(new ActionContextKey('clearCache'))) {
      this.cacheQuerySourceState = new PersistentCacheSourceStateIndexed(
        { maxNumTriples: this.cacheSizeNumTriples },
      );
    }

    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceStateQuerySource,
      this.cacheQuerySourceState,
      new SetSourceStateCache(),
    );
    const quadPatterns = this.extractQuadPatterns(action.context.getSafe(KeysInitQuery.query));

    cacheManager.registerCacheView(
      CacheSourceStateViews.cacheQueryView,
      new GetStreamingCacheView(
        quadPatterns, 
        (await this.mediatorFactoryAggregatedStore.mediate({ context })).aggregatedStore,
        context.get(KeysQueryOperation.unionDefaultGraph)),
    );
    
    return { context, operation: action.operation };

  }
  /**
   * Extracts all quad patterns from a given SPARQL Algebra AST.
   * @param ast The root operation node of the query.
   * @returns An array of all pattern nodes found in the AST.
   */
  private extractQuadPatterns(ast: Algebra.BaseOperation): Algebra.Pattern[] {
    const quadPatterns: Algebra.Pattern[] = [];

    visitOperation(ast, {
      [Algebra.Types.PATTERN]: {
        visitor: (node: Algebra.Pattern) => {
          quadPatterns.push(node);
        },
      },
    });

    return quadPatterns;
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

export class GetStreamingCacheView implements ICacheView<
  ISourceState, 
  { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink } | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation, context: IActionContext},
  BindingsStream | AsyncIterator<RDF.Quad> | ISourceState
> {
  protected traverseEnded = false;
  protected pendingCount = 0;

  protected DF: DataFactory = new DataFactory();
  protected BF: BindingsFactory = new BindingsFactory(this.DF);

  // protected cachedStore: StreamingStore<RDF.Quad>;
  protected cachedStore: IAggregatedStore;
  protected accumulatedSource: IQuerySource;

  protected readonly quadPatterns: Algebra.Pattern[];
  protected readonly unionDefaultGraph: boolean;
  protected allIteratorsClosedListener: () => void;

  public constructor(
    topLevelQuadPatterns: Algebra.Pattern[],
    aggregatedStore: IAggregatedStore,
    unionDefaultGraph?: boolean) {
    // this.cachedStore = new StreamingStore();
    this.cachedStore = aggregatedStore;

    this.quadPatterns = topLevelQuadPatterns;
    this.unionDefaultGraph = Boolean(unionDefaultGraph);
    
    this.allIteratorsClosedListener = () => this.checkForTermination()
    this.cachedStore.addAllIteratorsClosedListener(this.allIteratorsClosedListener);

    this.accumulatedSource = new QuerySourceRdfJs(
      <RDF.Source | RDF.DatasetCore> this.cachedStore,
      this.DF,
      this.BF,
    )
  }

  protected checkForTermination(cache?: IPersistentCache<ISourceState>){
    // If the query execution is marked as ended (when traversal ends for example)
    // and we finished importing all streams, we end the cached store.
    if (this.traverseEnded && this.pendingCount === 0) {
      if (!this.cachedStore.hasEnded()){
        this.cachedStore.end();
        if (cache){
          console.log(cache.endSession())
        }
        this.cachedStore.removeAllIteratorsClosedListener(this.allIteratorsClosedListener!);
      }
    }
  }

  public async construct(
    cache: IPersistentCache<ISourceState>,
    context: { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink} | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation, context: IActionContext},
  ): Promise<BindingsStream | AsyncIterator<RDF.Quad> | ISourceState | undefined> {
    if (context.mode === 'get') {
      // When passed end event, traversal is done and the source can end if we finished 
      // importing all data from previous 'get' requests to cache
      if (context.url === 'end'){
        this.traverseEnded = true;
        this.checkForTermination(cache);
        return;
      }

      const cacheEntry = await cache.get(context.url);
      // Only push if valid and policy satisfied
      if (cacheEntry && cacheEntry.cachePolicy?.satisfiesWithoutRevalidation(context.action)){
        this.pendingCount += this.quadPatterns.length;
        
        for (const quadPattern of this.quadPatterns){
          // Attach listeners to import to recalculate cMatch traversal criterion. 
          // This prevents having to call the metadata extraction mediator

          // Directly import only matching quads from source to the store.
          const importStream = this.cachedStore.import(
            cacheEntry.source.queryQuads(quadPattern, context.action.context)
          );
          importStream.on('end' , () => {
            this.pendingCount--;
            this.checkForTermination(cache);
          });
          importStream.on('error', (err) => {
            console.error('Import stream error:', err);
            this.pendingCount--;          // unblock termination
            this.checkForTermination(cache);
          });
        }
        
        // TODO update cache entry traverse metadata
        return cacheEntry;
      }
      return;
    }
    else if (context.mode === 'queryBindings') {
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        return this.accumulatedSource.queryBindings(context.operation, context.context)
      }
      throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
    }
    else if (context.mode === 'queryQuads') {
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        return this.accumulatedSource.queryQuads(context.operation, context.context);
      }
      throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
    }
    throw new Error(`Unknown view mode passed to ${this.constructor.name}: ${context.mode}`);
  }
}


export interface IActorOptimizeQueryOperationSetCacheQuerySourceArgs extends IActorOptimizeQueryOperationArgs {
  /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124000}
   */
  cacheSizeNumTriples: number;
  /**
   * A mediator for creating aggregated stores
   */
  mediatorFactoryAggregatedStore: MediatorFactoryAggregatedStore;
}

