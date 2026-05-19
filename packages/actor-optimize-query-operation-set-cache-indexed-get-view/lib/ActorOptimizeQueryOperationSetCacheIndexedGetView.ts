import type {
  IActionOptimizeQueryOperation,
  IActorOptimizeQueryOperationArgs,
  IActorOptimizeQueryOperationOutput,
} from '@comunica/bus-optimize-query-operation';
import {
  ActorOptimizeQueryOperation,
} from '@comunica/bus-optimize-query-operation';
import type { IActionQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching, KeysInitQuery, KeysQueryOperation, KeysQuerySourceIdentify } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type { ILink, ISourceState, ICacheView, IPersistentCache, ComunicaDataFactory } from '@comunica/types';

import { Algebra, AlgebraFactory } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import { UnionIterator } from 'asynciterator';
import { visitOperation } from '@comunica/utils-algebra/lib/utils';
import { ActorExtractLinksQuadPatternQuery } from '@comunica/actor-extract-links-quad-pattern-query';
import { QuerySourceFileLazy } from '@comunica/actor-query-source-identify-hypermedia-none-lazy/lib/QuerySourceFileLazy';
import { MediatorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheIndexedGetView extends ActorOptimizeQueryOperation {
  public readonly actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;
  public readonly mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;
  public readonly probabilityCacheMiss?: number;

  public constructor(args: IActorOptimizeQueryOperationSetCacheIndexedGetViewArgs) {
    super(args);
    this.mediatorQuerySourceIdentifyHypermedia = args.mediatorQuerySourceIdentifyHypermedia;
    this.actorExtractLinksQuadPatternQuery = args.actorExtractLinksQuadPatternQuery;
    this.probabilityCacheMiss = args.probabilityCacheMiss;

    console.log(`${this.name}: Created indexed cache view with probability miss: ${this.probabilityCacheMiss}`);
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    const context = action.context;
    if (!action.context.get(KeysQuerySourceIdentify.traverse)) {
      return { context, operation: action.operation };
    }

    const cacheManager = context.getSafe(KeysCaching.cacheManager);

    const dataFactory = context.getSafe(KeysInitQuery.dataFactory);
    const queryOp = context.getSafe(KeysInitQuery.query);
    const VAR = dataFactory.variable('__comunica:pp_var');

    const quadPatterns = this.extractQuadPatterns(action.context.getSafe(KeysInitQuery.query), dataFactory, VAR);

    cacheManager.registerCacheView(
      CacheSourceStateViews.indexedCacheGetView,
      new GetStreamingCacheView(
        action.context.getSafe(KeysInitQuery.dataFactory),
        quadPatterns,
        queryOp,
        this.mediatorQuerySourceIdentifyHypermedia,
        this.actorExtractLinksQuadPatternQuery,
        context.get(KeysQueryOperation.unionDefaultGraph),
        this.probabilityCacheMiss,
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

export class GetStreamingCacheView implements ICacheView<
  ISourceState,
  { 
    url: string; 
    action: IActionQuerySourceDereferenceLink,
    extractLinksQuadPattern?: boolean,
  },
  ISourceState
> {
  protected traverseEnded = false;
  protected pendingCount = 0;


  protected readonly dataFactory: ComunicaDataFactory;
  protected readonly bindingsFactory: BindingsFactory;
  protected readonly algebraFactory: AlgebraFactory;

  protected readonly quadPatterns: Algebra.Pattern[];
  protected readonly queryOperation: Algebra.Operation;
  protected readonly unionDefaultGraph: boolean;

  protected readonly actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;
  protected readonly mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;

  protected readonly probabilityCacheMiss?: number;
  protected simulatedMisses: number = 0;
  protected hits: number = 0;

  public constructor(
    dataFactory: ComunicaDataFactory,
    topLevelQuadPatterns: Algebra.Pattern[],
    queryOperation: Algebra.Operation,
    mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia,
    actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery,
    unionDefaultGraph?: boolean,
    probabilityCacheMiss?: number,
  ) {
    this.dataFactory = dataFactory;
    this.bindingsFactory = new BindingsFactory(this.dataFactory);
    this.algebraFactory = new AlgebraFactory(this.dataFactory);

    this.quadPatterns = topLevelQuadPatterns;
    this.queryOperation = queryOperation;
    this.unionDefaultGraph = Boolean(unionDefaultGraph);

    this.actorExtractLinksQuadPatternQuery = actorExtractLinksQuadPatternQuery;
    this.mediatorQuerySourceIdentifyHypermedia = mediatorQuerySourceIdentifyHypermedia
    this.probabilityCacheMiss = probabilityCacheMiss;
  }

  public async construct(
    cache: IPersistentCache<ISourceState>,
    context: { 
      url: string;
      action: IActionQuerySourceDereferenceLink;
      extractLinksQuadPattern?: boolean 
    } 
  ): Promise<ISourceState | undefined> {
      const cacheEntry = await cache.get(context.url);
      // Only push if valid and policy satisfied
      if (cacheEntry && cacheEntry.cachePolicy?.satisfiesWithoutRevalidation(context.action)) {
        // Code to simulate cache misses, should not be in final code.
        this.hits++
        if (this.probabilityCacheMiss){
          if (Math.random() < this.probabilityCacheMiss){
            this.simulatedMisses++
            return;
          }
          
        }

        this.pendingCount += this.quadPatterns.length;

        // Re-extract query dependent traverse entries when required. 
        if (context.extractLinksQuadPattern && this.actorExtractLinksQuadPatternQuery) {
          const links: ILink[] = [];
          const quads = new UnionIterator(this.quadPatterns.map(
            (quadPattern) => cacheEntry.source.queryQuads(
              quadPattern, 
              context.action.context,
            )
          ), { "autoStart": false });

          const patternLinks = await ActorExtractLinksQuadPatternQuery
            .collectStream(quads, (quad, arr) => {
              ActorExtractLinksQuadPatternQuery.extractLinksOnQuad(
                quad,
                arr,
                this.queryOperation,
                true,
                this.actorExtractLinksQuadPatternQuery!.name,
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
            context: context.action.context,
            url: context.url,
            metadata: {},
          })).source,
        );
        
        return {...cacheEntry, source}
      }
      return
    }
}

export interface IActorOptimizeQueryOperationSetCacheIndexedGetViewArgs extends IActorOptimizeQueryOperationArgs {
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
  /**
   * For simulating query misses
   * @range {float}
   */
  probabilityCacheMiss?: number;
}

/**
 * Interface containing data for offline traversal
 */
export interface IOfflineTraversalEntry {
  /**
   * The traversal entries depending on predicates in the query
   */
  predicates: Record<string, ILink>;
  /**
   * The traversal entries independent of the query
   */
  default: ILink[];
}