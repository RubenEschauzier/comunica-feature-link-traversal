import type { PersistentCacheManager } from '@comunica/actor-context-preprocess-set-persistent-cache-manager';
import type { IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries';
import type { TestResult, IActorTest } from '@comunica/core';
import { passTestVoid, ActionContext } from '@comunica/core';
import type { ICacheView, ILink, IPersistentCache, ISourceState } from '@comunica/types';
import { IOfflineTraversalEntry } from '@comunica/types-link-traversal';
import { Algebra, algebraUtils, isKnownOperation } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';

/**
 * A comunica Set Cache Count View Context Preprocess Actor.
 */
export class ActorContextPreprocessSetCacheCountViewOfflineTraversal extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(_action: IActionContextPreprocess): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    const context = action.context;
    const cacheManager: PersistentCacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCacheView(
      CacheSourceStateViews.indexedCacheCountViewOfflineTraversal,
      new CacheCountViewOfflineTraversal(),
    );
    return { context };
  }
}

export class CacheCountViewOfflineTraversal
implements ICacheView<ISourceState, { 
  operation: Algebra.Operation,
  seeds: ILink[], 
  query: Algebra.BaseOperation,
 }, number> {
  protected readonly computedCounts: Record<string, number> = {};
  protected reachableDocuments: Set<string> | undefined;

  public async construct(
    cache: IPersistentCache<ISourceState>,
    context: { operation: Algebra.Operation; seeds: ILink[], query: Algebra.BaseOperation },
  ): Promise<number | undefined> {
    if (!isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
      throw new Error('Count view only accepts quad patterns');
    }

    if (!context.seeds){
      throw new Error(`Invalid context missing seeds argument: context: ${context}`);
    }
    if (!context.query){
      throw new Error(`Invalid context missing query argument: context: ${context}`);
    }

    const pattern = context.operation;
    const patternKey = this.patternKey(pattern);

    if (patternKey in this.computedCounts) {
      return this.computedCounts[patternKey];
    }

    // Compute reachable documents if this hasn't been computed yet 
    // in previous executions and if any of the seed urls is in the cache
    if (!this.reachableDocuments 
      && context.seeds.some(seed => cache.has(seed.url))){
        this.reachableDocuments = await this.findReachableDocuments(context.query, context.seeds, cache);
    }
    
    let totalCount = 0;
    const cacheEntryStream = cache.entries();

    for await (const [ key, source ] of cacheEntryStream) {
      if (source.source.countQuads) {
        // Skip any non-reachable documents if we have computed this
        // When no seeds are present we use all documents to approximate
        // the new subweb.
        if (this.reachableDocuments && !this.reachableDocuments.has(key)){
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
    cache: IPersistentCache<ISourceState>
  ): Promise<Set<string>> {
    const predicatesInQuery = this.getPredicatesFromQuery(query);
    const reachableDocuments: Set<string> = new Set();
    const toVisit: ILink[] = [...seeds];

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

      const nextLinks: IOfflineTraversalEntry = sourceState.metadata["offlineTraversal"];
      if (nextLinks === undefined) {
        throw new Error("Found cached document without traversal information");
      }

      // Always follow default entries
      for (const link of nextLinks.default) {
        if (!reachableDocuments.has(link.url)) {
          toVisit.push(link);
        }
      }

      // Only follow predicate entries that match predicates in the query
      for (const [predicate, link] of Object.entries(nextLinks.predicates)) {
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
  protected getPredicatesFromQuery(query: Algebra.BaseOperation){
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
