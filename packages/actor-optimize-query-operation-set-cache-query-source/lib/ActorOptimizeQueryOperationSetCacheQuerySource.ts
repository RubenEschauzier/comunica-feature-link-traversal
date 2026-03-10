import { CacheEntrySourceState, CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching, KeysInitQuery, KeysQueryOperation } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContext, ActionContextKey, passTestVoid } from '@comunica/core';
import type {  BindingsStream, ISourceState } from '@comunica/types';

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

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheQuerySource extends ActorOptimizeQueryOperation {
  private readonly cacheSizeNumTriples: number;
  private cacheQuerySourceState: PersistentCacheSourceStateIndexed;

  public constructor(args: IActorOptimizeQueryOperationSetCacheQuerySourceArgs) {
    super(args);
    this.cacheSizeNumTriples = args.cacheSizeNumTriples
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
      console.log("Here!")
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
      new GetStreamingCacheView(quadPatterns, context.get(KeysQueryOperation.unionDefaultGraph)),
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
  { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink } | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
  BindingsStream | AsyncIterator<RDF.Quad> | ISourceState
> {
  protected ended = false;
  protected pendingCount = 0;

  protected DF: DataFactory = new DataFactory();
  protected BF: BindingsFactory = new BindingsFactory(this.DF);

  protected cachedStore: StreamingStore<RDF.Quad>;

  protected readonly quadPatterns: Algebra.Pattern[];
  protected readonly unionDefaultGraph: boolean;

  protected isEnded: boolean = false;
  protected nImportingStreams = 0;

  public constructor(topLevelQuadPatterns: Algebra.Pattern[], unionDefaultGraph?: boolean) {
    this.cachedStore = new StreamingStore();

    this.quadPatterns = topLevelQuadPatterns;
    this.unionDefaultGraph = Boolean(unionDefaultGraph);
  }

  protected checkForTermination(){
    // If the query execution is marked as ended (when traversal ends for example)
    // and we finished importing all streams, we end the cached store.
    if (this.ended && this.pendingCount === 0) {
      this.cachedStore.end();
    }
  }

  public async construct(
    cache: IPersistentCache<ISourceState>,
    context: { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink} | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
  ): Promise<BindingsStream | AsyncIterator<RDF.Quad> | ISourceState | undefined> {
    if (context.mode === 'get') {
        // When passed end event, traversal is done and the source can end if we finished 
        // importing all data from previous 'get' requests to cache
        if (context.url === 'end'){
          if (!this.ended) {
            this.ended = true;
            this.checkForTermination();
          }
          return;
        }

        const cacheEntry = await cache.get(context.url);
        // Only push if valid and policy satisfied
        if (cacheEntry && cacheEntry.cachePolicy?.satisfiesWithoutRevalidation(context.action)){
          for (const quadPattern of this.quadPatterns){
            // Directly import only matching quads from source to the store.
            this.pendingCount++
            const importStream = this.cachedStore.import(
              cacheEntry.source.queryQuads(quadPattern, new ActionContext())
            );
            importStream.on('end' , () => {
              this.pendingCount--;
              this.checkForTermination();
            })
          }
          return cacheEntry;
        }
        return;
    }
    else if (context.mode === 'queryBindings') {
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        const result = this.cachedStore.match(
          context.operation.subject,
          context.operation.predicate,
          context.operation.object,
          context.operation.graph,
        );
        return quadsToBindings(
          wrapAsyncIterator(result, { "autoStart": false }),
          context.operation,
          this.DF,
          this.BF,
          this.unionDefaultGraph,
        );
      }
      throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
    }
    else if (context.mode === 'queryQuads') {
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        return wrapAsyncIterator(this.cachedStore.match(
          context.operation.subject,
          context.operation.predicate,
          context.operation.object,
          context.operation.graph,
        ));
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
}

