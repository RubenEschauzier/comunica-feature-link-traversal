import { ActorExtractLinksQuadPatternQuery } from '@comunica/actor-extract-links-quad-pattern-query';
import { QuerySourceFileLazy } from '@comunica/actor-query-source-identify-hypermedia-none-lazy/lib/QuerySourceFileLazy';
import type {
  IActionOptimizeQueryOperation,
  IActorOptimizeQueryOperationArgs,
  IActorOptimizeQueryOperationOutput,
} from '@comunica/bus-optimize-query-operation';
import {
  ActorOptimizeQueryOperation,
} from '@comunica/bus-optimize-query-operation';
import type { IActionQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import type { MediatorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import { CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching, KeysInitQuery, KeysQueryOperation, KeysQuerySourceIdentify } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContext, passTestVoid } from '@comunica/core';
import type { ILink, ISourceState, ICacheView, IPersistentCache, ComunicaDataFactory } from '@comunica/types';

import { Algebra, algebraUtils, isKnownOperation } from '@comunica/utils-algebra';
import { visitOperation } from '@comunica/utils-algebra/lib/utils';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import { UnionIterator } from 'asynciterator';

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheCsetGetView extends ActorOptimizeQueryOperation {
  public readonly actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;
  public readonly mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;
  public readonly probabilityCacheMiss?: number;

  public constructor(args: IActorOptimizeQueryOperationSetCacheCsetGetViewArgs) {
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
      if (seenPredicates.has(predicate.value)) {
        return;
      }
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

export class CacheCountViewOfflineTraversal
implements ICacheView<
  ISourceState,
  ISourceState,
  {
    operation: Algebra.Operation;
    seeds: ILink[];
    query: Algebra.BaseOperation;
  },
number
> {
  protected readonly computedCounts: Record<string, number> = {};
  protected reachableDocuments: Set<string> | undefined;

  public async construct(
    cache: IPersistentCache<ISourceState, ISourceState>,
    context: { operation: Algebra.Operation; seeds: ILink[]; query: Algebra.BaseOperation },
  ): Promise<number | undefined> {
    if (!isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
      throw new Error('Count view only accepts quad patterns');
    }

    if (!context.seeds) {
      throw new Error(`Invalid context missing seeds argument: context: ${context}`);
    }
    if (!context.query) {
      throw new Error(`Invalid context missing query argument: context: ${context}`);
    }

    const pattern = context.operation;
    const patternKey = this.patternKey(pattern);

    if (patternKey in this.computedCounts) {
      return this.computedCounts[patternKey];
    }

    // Compute reachable documents if this hasn't been computed yet
    // in previous executions and if any of the seed urls is in the cache
    if (!this.reachableDocuments &&
      context.seeds.some(seed => cache.has(seed.url))) {
      this.reachableDocuments = await this.findReachableDocuments(context.query, context.seeds, cache);
    }

    let totalCount = 0;
    const cacheEntryStream = cache.entries();

    for await (const [ key, source ] of cacheEntryStream) {
      if (source.source.countQuads) {
        // Skip any non-reachable documents if we have computed this
        // When no seeds are present we use all documents to approximate
        // the new subweb.
        if (this.reachableDocuments && !this.reachableDocuments.has(key)) {
          continue;
        }
        const quadCount = await source.source.countQuads(context.operation, new ActionContext());
        totalCount += quadCount;
      }
    }
    this.computedCounts[patternKey] = totalCount;
    return totalCount;
  }

  protected async findReachableDocuments(
    query: Algebra.BaseOperation,
    seeds: ILink[],
    cache: IPersistentCache<ISourceState, ISourceState>,
  ): Promise<Set<string>> {
    const predicatesInQuery = this.getPredicatesFromQuery(query);
    const reachableDocuments: Set<string> = new Set();
    const toVisit: ILink[] = [ ...seeds ];

    while (toVisit.length > 0) {
      const current = toVisit.pop()!;

      if (reachableDocuments.has(current.url)) {
        continue;
      }

      const sourceState = await cache.get(current.url);
      if (!sourceState) {
        continue;
      }

      reachableDocuments.add(current.url);

      const nextLinks: IOfflineTraversalEntry = sourceState.metadata.offlineTraversal;
      if (nextLinks === undefined) {
        console.log(sourceState.metadata);
        console.log(sourceState.link);
        throw new Error('Found cached document without traversal information');
      }

      // Always follow default entries
      for (const link of nextLinks.default) {
        if (!reachableDocuments.has(link.url)) {
          toVisit.push(link);
        }
      }

      // Only follow predicate entries that match predicates in the query
      for (const [ predicate, link ] of Object.entries(nextLinks.predicates)) {
        if (predicatesInQuery.has(predicate) && !reachableDocuments.has(link.url)) {
          toVisit.push(link);
        }
      }
    }

    return reachableDocuments;
  }

  /**
   * Get all predicates from query to determine what links we can follow
   */
  protected getPredicatesFromQuery(query: Algebra.BaseOperation) {
    const predicates: Set<string> = new Set();
    algebraUtils.visitOperation(query, {
      [Algebra.Types.PATTERN]: {
        preVisitor: () => ({ continue: false }),
        visitor: (pattern) => {
          if (pattern.predicate.termType === 'NamedNode') {
            predicates.add(pattern.predicate.value);
          }
        },
      },
      [Algebra.Types.PATH]: {
        preVisitor: () => ({ continue: false }),
        visitor: (path: Algebra.Path) => {
          algebraUtils.visitOperation(path, {
            [Algebra.Types.LINK]: {
              preVisitor: () => ({ continue: false }),
              visitor: (link: Algebra.Link) => {
                predicates.add(link.iri.value);
              },
            },
            [Algebra.Types.NPS]: {
              preVisitor: () => ({ continue: false }),
              visitor: (nps: Algebra.Nps) => {
                for (const iri of nps.iris) {
                  predicates.add(iri.value);
                }
              },
            },
          });
        },
      },
    });
    return predicates;
  }

  private patternKey(pattern: Algebra.Pattern): string {
    return [
      pattern.subject.value,
      pattern.predicate.value,
      pattern.object.value,
      pattern.graph?.value ?? '',
    ].join('|');
  }
}


export interface IActorOptimizeQueryOperationSetCacheCsetGetViewArgs extends IActorOptimizeQueryOperationArgs {
  /**
   * Test
   */
  mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;
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
