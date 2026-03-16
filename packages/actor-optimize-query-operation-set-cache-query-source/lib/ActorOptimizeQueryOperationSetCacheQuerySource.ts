import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';
import type { MediatorFactoryAggregatedStore } from '@comunica/bus-factory-aggregated-store';
import type {
  IActionOptimizeQueryOperation,
  IActorOptimizeQueryOperationArgs,
  IActorOptimizeQueryOperationOutput,
} from '@comunica/bus-optimize-query-operation';
import {
  ActorOptimizeQueryOperation,
} from '@comunica/bus-optimize-query-operation';
import type { IActionQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { CacheEntrySourceState, CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching, KeysInitQuery, KeysQueryOperation, KeysQuerySourceIdentify } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContext, ActionContextKey, passTestVoid } from '@comunica/core';
import type { BindingsStream, IActionContext, ILink, IQuerySource, ISourceState, ICacheView, IPersistentCache, ISetFn, ComunicaDataFactory } from '@comunica/types';

import type { IAggregatedStore } from '@comunica/types-link-traversal';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import { AsyncIterator, UnionIterator } from 'asynciterator';
import { PersistentCacheSourceStateIndexed } from './PersistentCacheSourceStateIndexed';
import { visitOperation } from '@comunica/utils-algebra/lib/utils';
import { ActorExtractLinksQuadPatternQuery } from '@comunica/actor-extract-links-quad-pattern-query';
import { QuerySourceFileLazy } from '@comunica/actor-query-source-identify-hypermedia-none-lazy/lib/QuerySourceFileLazy';
import { MediatorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheQuerySource extends ActorOptimizeQueryOperation {
  private cacheQuerySourceState: PersistentCacheSourceStateIndexed;
  private readonly cacheSizeNumTriples: number;

  public readonly mediatorFactoryAggregatedStore: MediatorFactoryAggregatedStore;
  public readonly actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;
  public readonly mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;

  public constructor(args: IActorOptimizeQueryOperationSetCacheQuerySourceArgs) {
    super(args);
    this.cacheSizeNumTriples = args.cacheSizeNumTriples;
    this.mediatorFactoryAggregatedStore = args.mediatorFactoryAggregatedStore;
    this.mediatorQuerySourceIdentifyHypermedia = args.mediatorQuerySourceIdentifyHypermedia;
    this.actorExtractLinksQuadPatternQuery = args.actorExtractLinksQuadPatternQuery;

    this.cacheQuerySourceState = new PersistentCacheSourceStateIndexed(
      { maxNumTriples: args.cacheSizeNumTriples },
    );
    console.log(`Created cache with maxSize: ${args.cacheSizeNumTriples}`)
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    const context = action.context;
    if (!action.context.get(KeysQuerySourceIdentify.traverse)) {
      return { context, operation: action.operation };
    }

    if (context.get(KeysCaching.clearCache) || context.get(new ActionContextKey('clearCache'))) {
      this.cacheQuerySourceState = new PersistentCacheSourceStateIndexed(
        { maxNumTriples: this.cacheSizeNumTriples },
      );
      console.log(`Cleaned cache, size: ${await this.cacheQuerySourceState.size()}`);
    }

    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceStateQuerySource,
      this.cacheQuerySourceState,
      new SetSourceStateCache(),
    );
    const dataFactory = action.context.getSafe(KeysInitQuery.dataFactory);
    const VAR = dataFactory.variable('__comunica:pp_var');
    const quadPatterns = this.extractQuadPatterns(action.context.getSafe(KeysInitQuery.query), dataFactory, VAR);

    cacheManager.registerCacheView(
      CacheSourceStateViews.cacheQueryView,
      new GetStreamingCacheView(
        action.context.getSafe(KeysInitQuery.dataFactory),
        quadPatterns,
        ActorExtractLinksQuadPatternQuery.getCurrentQuery(action.context)!,
        (await this.mediatorFactoryAggregatedStore.mediate({ context })).aggregatedStore,
        this.mediatorQuerySourceIdentifyHypermedia,
        this.actorExtractLinksQuadPatternQuery,
        context.get(KeysQueryOperation.unionDefaultGraph),
      ),
    );

    return { context, operation: action.operation };
  }


  private extractQuadPatterns(
    ast: Algebra.BaseOperation,
    dataFactory: ComunicaDataFactory,
    VAR: RDF.Variable,
  ): Algebra.Pattern[] {
    const quadPatterns: Algebra.Pattern[] = [];
    const seenPredicates = new Set<string>();
    let pathIndex = 0;

    const addSyntheticPattern = (predicate: RDF.NamedNode, graph: RDF.Term): void => {
      if (seenPredicates.has(predicate.value)) return;
      seenPredicates.add(predicate.value);
      const idx = ++pathIndex;
      quadPatterns.push({
        type: Algebra.Types.PATTERN,
        subject: dataFactory.variable(`_path_s_${idx}`),
        predicate,
        object: dataFactory.variable(`_path_o_${idx}`),
        graph,
      } as Algebra.Pattern);
    };

    visitOperation(ast, {
      [Algebra.Types.PATTERN]: {
        preVisitor: () => ({ continue: false }),
        visitor: (node: Algebra.Pattern): Algebra.Pattern => {
          quadPatterns.push(node);
          return node;
        },
      },
      [Algebra.Types.PATH]: {
        preVisitor: () => ({ continue: false }),
        visitor: (pathNode: Algebra.Path): Algebra.Path => {
          // Re-enter the path expression tree to find all LINK and NPS leaves
          visitOperation(pathNode, {
            [Algebra.Types.LINK]: {
              preVisitor: () => ({ continue: false }),
              visitor: (link: Algebra.Link): Algebra.Link => {
                addSyntheticPattern(link.iri, pathNode.graph);
                return link;
              },
            },
            [Algebra.Types.NPS]: {
              preVisitor: () => ({ continue: false }),
              visitor: (nps: Algebra.Nps): Algebra.Nps => {
                // NPS iris are the *excluded* predicates — we cannot know what
                // *will* match, so fall back to a wildcard pattern for this path
                quadPatterns.push({
                  type: Algebra.Types.PATTERN,
                  subject: VAR,
                  predicate: VAR,
                  object: VAR,
                  graph: pathNode.graph,
                } as Algebra.Pattern);
                return nps;
              },
            },
          });
          return pathNode;
        },
      },
    });

    return quadPatterns;
  }
}

export class SetSourceStateCache implements ISetFn<ISourceState, ISourceState, { headers: Headers }> {
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
  { url: string; mode: 'get'; action: IActionQuerySourceDereferenceLink } | { mode: 'queryBindings' | 'queryQuads'; operation: Algebra.Operation; context: IActionContext },
  BindingsStream | AsyncIterator<RDF.Quad> | ISourceState
> {
  protected traverseEnded = false;
  protected pendingCount = 0;

  protected readonly dataFactory: ComunicaDataFactory;
  protected readonly bindingsFactory: BindingsFactory;
  protected readonly algebraFactory: AlgebraFactory;

  protected readonly cachedStore: IAggregatedStore;
  protected readonly accumulatedSource: IQuerySource;

  protected readonly quadPatterns: Algebra.Pattern[];
  protected readonly queryOperation: Algebra.Operation;
  protected readonly unionDefaultGraph: boolean;
  protected allIteratorsClosedListener: () => void;

  protected readonly actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;
  protected readonly mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;

  countsPerPattern: Record<string, number> = {}

  public constructor(
    dataFactory: ComunicaDataFactory,
    topLevelQuadPatterns: Algebra.Pattern[],
    queryOperation: Algebra.Operation,
    aggregatedStore: IAggregatedStore,
    mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia,
    actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery,
    unionDefaultGraph?: boolean,
  ) {
    this.dataFactory = dataFactory;
    this.bindingsFactory = new BindingsFactory(this.dataFactory);
    this.algebraFactory = new AlgebraFactory(this.dataFactory);
    this.cachedStore = aggregatedStore;

    this.quadPatterns = topLevelQuadPatterns;
    for (const pattern of this.quadPatterns){
      this.countsPerPattern[JSON.stringify(pattern, null, 2)] = 0;
    }
    this.queryOperation = queryOperation;
    this.unionDefaultGraph = Boolean(unionDefaultGraph);

    this.allIteratorsClosedListener = () => this.checkForTermination();
    this.cachedStore.addAllIteratorsClosedListener(this.allIteratorsClosedListener);

    this.accumulatedSource = new QuerySourceRdfJs(
      <RDF.Source | RDF.DatasetCore> this.cachedStore,
      this.dataFactory,
      this.bindingsFactory,
    );

    this.actorExtractLinksQuadPatternQuery = actorExtractLinksQuadPatternQuery;
    this.mediatorQuerySourceIdentifyHypermedia = mediatorQuerySourceIdentifyHypermedia
  }

  protected checkForTermination(cache?: IPersistentCache<ISourceState>) {
    // If the query execution is marked as ended (when traversal ends for example)
    // and we finished importing all streams, we end the cached store.
    if (this.traverseEnded && this.pendingCount === 0 && !this.cachedStore.hasEnded()) {
      this.cachedStore.end();
      if (cache) {
        console.log(cache.endSession());
      }
      this.cachedStore.removeAllIteratorsClosedListener(this.allIteratorsClosedListener);
    }
  }

  public async construct(
    cache: IPersistentCache<ISourceState>,
    context: { url: string; mode: 'get'; action: IActionQuerySourceDereferenceLink; extractLinksQuadPattern?: boolean } | { mode: 'queryBindings' | 'queryQuads'; operation: Algebra.Operation; context: IActionContext },
  ): Promise<BindingsStream | AsyncIterator<RDF.Quad> | ISourceState | undefined> {
    if (context.mode === 'get') {
      // When passed end event, traversal is done and the source can end if we finished
      // importing all data from previous 'get' requests to cache
      if (context.url === 'end') {
        this.traverseEnded = true;
        this.checkForTermination(cache);
        return;
      }

      const cacheEntry = await cache.get(context.url);
      // Only push if valid and policy satisfied
      if (cacheEntry && cacheEntry.cachePolicy?.satisfiesWithoutRevalidation(context.action)) {
        this.pendingCount += this.quadPatterns.length;
        
        // Re-extract query dependent traverse entries when required. 
        if (context.extractLinksQuadPattern && this.actorExtractLinksQuadPatternQuery) {
          const links: ILink[] = [];
          const quads = new UnionIterator(this.quadPatterns.map(
            (quadPattern) => cacheEntry.source.queryQuads(quadPattern, 
              new ActionContext().set(KeysQueryOperation.unionDefaultGraph, true))
          ), { "autoStart": false });

          const patternLinks = await ActorExtractLinksQuadPatternQuery
            .collectStream(quads, (quad, arr) => {
              ActorExtractLinksQuadPatternQuery.extractLinksOnQuad(
                quad,
                arr,
                this.queryOperation,
                true,
                this.constructor.name,
              );
          });

          links.push(...patternLinks);
          const staticTraverseEntries = cacheEntry.metadata.traverse.filter(
            (x: ILink) => x.metadata?.producedByActor.name !== this.actorExtractLinksQuadPatternQuery!.name
          );
          cacheEntry.metadata.traverse = [...staticTraverseEntries, ...links];
        }

        const matchingQuads = await Promise.all(this.quadPatterns.map((quadPattern) => 
          cacheEntry.source.queryQuads(quadPattern, context.action.context)
        ));

        const source = new QuerySourceFileLazy(
          new UnionIterator(matchingQuads, { "autoStart": false}),
          this.dataFactory,
          context.url,
          async quads => (await this.mediatorQuerySourceIdentifyHypermedia.mediate({
            quads,
            context: new ActionContext(),
            url: context.url,
            metadata: {},
          })).source,
        );
        
        return {...cacheEntry, source}
      }
      return
    }

    if (context.mode === 'queryBindings') {
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        return this.accumulatedSource.queryBindings(context.operation, context.context);
      }
      throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
    } else if (context.mode === 'queryQuads') {
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
  /**
   * Test
   */
  mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia
  /**
   * Optional actor to execute cMatch traversal criterion on cached sources.
   * This should always be passed when cMatch is used, as cached sources contain stale
   * traversal metadata entries otherwise.
   */
  actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;
}
