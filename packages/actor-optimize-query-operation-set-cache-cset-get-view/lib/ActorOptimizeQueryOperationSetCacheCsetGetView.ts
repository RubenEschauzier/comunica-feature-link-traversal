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
import { CacheDataSummariesViews, CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching, KeysInitQuery, KeysQueryOperation, KeysQuerySourceIdentify } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContext, passTestVoid } from '@comunica/core';
import type { ILink, ISourceState, ICacheView, IPersistentCache, ComunicaDataFactory } from '@comunica/types';

import { Algebra, algebraUtils, isKnownOperation } from '@comunica/utils-algebra';
import { visitOperation } from '@comunica/utils-algebra/lib/utils';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import { UnionIterator } from 'asynciterator';
import { ICharacteristicPair, ICharacteristicSet, IDataSummary } from '@comunica/actor-optimize-query-operation-set-cache-cset-offline-traversal';

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheCsetGetView extends ActorOptimizeQueryOperation {
  public readonly actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;
  public readonly mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;
  public readonly maxNumCsets: number;
  public readonly probabilityCacheMiss?: number;

  public constructor(args: IActorOptimizeQueryOperationSetCacheCsetGetViewArgs) {
    super(args);
    this.mediatorQuerySourceIdentifyHypermedia = args.mediatorQuerySourceIdentifyHypermedia;
    this.actorExtractLinksQuadPatternQuery = args.actorExtractLinksQuadPatternQuery;
    this.maxNumCsets = args.maxNumCsets;
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

    cacheManager.registerCacheView(
      CacheDataSummariesViews.cacheCsetCpEstimationView,
      new CacheCsetViewOfflineTraversal(this.maxNumCsets),
    );
    console.log(`Register view: ${CacheDataSummariesViews.cacheCsetCpEstimationView.id}`)

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

export class CacheCsetViewOfflineTraversal
implements ICacheView<
  ISourceState,
  IDataSummary,
  {
    seeds: ILink[];
    query: Algebra.BaseOperation;
  },
  IReachableDataSummary
> {
  protected readonly computedCounts: Record<string, number> = {};
  protected reachableDocuments: Set<string> | undefined;
  protected globalDataSummary: IReachableDataSummary | undefined;
  protected readonly maxNumCsets: number;


  public constructor(maxNumCsets: number){
    this.maxNumCsets = maxNumCsets;
  }


  public async construct(
    cache: IPersistentCache<ISourceState, IDataSummary>,
    context: { seeds: ILink[]; query: Algebra.BaseOperation },
  ): Promise<IReachableDataSummary> {
    // TODO if subject stars work: Implement same thing for object stars too, its a similar approach
    // I just need to know its worthwhile
    // TODO: Possible optimization using integers instead of strings as keys
    // TODO: Maybe use triple terms instead of named graphs for document indication in store-based
    // we can delete by querying the document object and then extracting all triples and doing a
    // delete.

    if (!context.query) {
      throw new Error(`Invalid context missing query argument: context: ${context}`);
    }

    // Compute reachable documents if this hasn't been computed yet
    // in previous executions and if any of the seed urls is in the cache
    if (!this.reachableDocuments &&
      context.seeds.some(seed => cache.has(seed.url))) {
      this.reachableDocuments = await this.findReachableDocuments(context.query, context.seeds, cache);
    }

    const cacheEntryStream = cache.entries();
    if (!this.globalDataSummary) {
      const globalCsetKeysSorted: ICsetPredicateKey[] = [];
      const globalCps = new Map<string, ICharacteristicPair>();
      const globalCsets = new Map<string, ICharacteristicSet>();
      const globalSubjectsToCsets = new Map<number, ICharacteristicSet>();
      const globalPredToCsets = new Map<string, Set<string>>();
      for await (const [ key, summary ] of cacheEntryStream) {

        if (this.reachableDocuments && !this.reachableDocuments.has(key)){
          continue;
        }
        // Aggregate the csets in different documents to one global cset mapping
        for (const [predKey, cset] of summary.csets.entries()){

          let globalCsetEntry = globalCsets.get(predKey);
          if (!globalCsetEntry) {
            globalCsetKeysSorted.push({
              predicateKey: predKey,
              sizeCset: cset.predicateCounts.size,
            });

            globalCsetEntry = {
              predKey,
              subjCount: 0,
              predicateCounts: new Map(
                Array.from(cset.predicateCounts.keys()).map(k => [k, 0])
              ),
              localSubjects: new Set(),
              localObjects: new Map(
                Array.from(cset.localObjects.keys()).map(k => [k, new Set()])
              ),
            };

            globalCsets.set(predKey, globalCsetEntry);
          }    
          globalCsetEntry.subjCount += cset.subjCount;
          for (const [pred, count] of cset.predicateCounts.entries()) {
            const currentCount = globalCsetEntry.predicateCounts.get(pred) || 0;
            globalCsetEntry.predicateCounts.set(pred, currentCount + count);
          }

          // Aggregate local subjects (Union of Sets)
          for (const subj of cset.localSubjects) {
            globalCsetEntry.localSubjects.add(subj);
          }

          // Aggregate local objects per predicate (Union of Sets)
          for (const [pred, objects] of cset.localObjects.entries()) {
            let globalObjSet = globalCsetEntry.localObjects.get(pred);
            if (!globalObjSet) {
              globalObjSet = new Set();
              globalCsetEntry.localObjects.set(pred, globalObjSet);
            }
            for (const obj of objects) {
              globalObjSet.add(obj);
            }
          }

          // Add predicate mapping
          for (const singlePredKey of cset.predicateCounts.keys()){
            let predCsets: Set<string> | undefined = globalPredToCsets.get(singlePredKey);
            if (!predCsets){
              predCsets = new Set<string>();
              globalPredToCsets.set(singlePredKey, predCsets);
            }
            predCsets.add(predKey);
          }

        }
        for (const [cpKey, cp] of summary.cps.entries()){
          let globalCpEntry = globalCps.get(cpKey);
          if (!globalCpEntry){
            globalCpEntry = {
              ...cp,
              count: 0
            }
            globalCps.set(cpKey, globalCpEntry);
          }
          globalCpEntry.count += cp.count;
        }
      }

      // Compute characteristic pairs between different reachable documents
      const entityResolutionMap = new Map<number, string>();
      // Map subjects to their characteristic sets
      for (const [csetKey, cset] of globalCsets.entries()) {
        for (const subjectHash of cset.localSubjects) {
          entityResolutionMap.set(subjectHash, csetKey);
          globalSubjectsToCsets.set(subjectHash, cset);
        }
      }
      
      for (const [subjectCSetKey, cset] of globalCsets.entries()) {
        for (const [predicateKey, objectHashes] of cset.localObjects.entries()) {
          for (const objectHash of objectHashes) {
            const objectCSetKey = entityResolutionMap.get(objectHash);
            // If we match an entity in the subjects with one of the objects in current
            // cs we have need to update this cp
            if (objectCSetKey) {
              const cpKey = this.toCpKey(subjectCSetKey, predicateKey, objectCSetKey);
              let cp = globalCps.get(cpKey);
              if (!cp){
                cp = {
                  csetSubj: cset,
                  csetObj: globalCsets.get(objectCSetKey)!,
                  predicate: predicateKey,
                  count: 0
                }
                globalCps.set(cpKey, cp);
              }
              cp.count++;
            }
          }
        }
      }
      console.log(`Contains: ${globalCsets.size} csets`);
      console.log(`Contains: ${globalCps.size} cps`);
      this.globalDataSummary = {
        csets: globalCsets,
        cps: globalCps,
        subjectToCset: globalSubjectsToCsets,
        predToCset: globalPredToCsets,
      }
    }
    return this.globalDataSummary;
  }

  protected async findReachableDocuments(
    query: Algebra.BaseOperation,
    seeds: ILink[],
    cache: IPersistentCache<ISourceState, IDataSummary>,
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

      const nextLinks: IOfflineTraversalEntry = sourceState.offlineTraversal;
      if (nextLinks === undefined) {
        console.log(sourceState);
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
  /**
   * From PersistentCacheCset and should always be aligned (possibly add to util functions)
   * @param subjKey 
   * @param predicateKey 
   * @param objectKey 
   * @returns 
   */
  private toCpKey(subjKey: string, predicateKey: string, objectKey: string){
    return `${subjKey}|${predicateKey}|${objectKey}`;
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
   * Maximum number of csets to use for optimization, anything more will be merged
   */
  maxNumCsets: number;
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

export interface IReachableDataSummary{
  cps: Map<string, ICharacteristicPair>;
  csets: Map<string, ICharacteristicSet>;
  subjectToCset: Map<number, ICharacteristicSet>;
  predToCset: Map<string, Set<string>>;
}

export interface ICsetPredicateKey {
  /**
   * The actual key
   */
  predicateKey: string,
  /**
   * Number of predicates in the key
   */
  sizeCset: number
}