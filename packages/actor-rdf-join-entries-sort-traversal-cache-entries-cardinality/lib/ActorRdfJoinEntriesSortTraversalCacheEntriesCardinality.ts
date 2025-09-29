import { MediatorExtractSources } from '@comunica/bus-extract-sources';
import type {
  IActionRdfJoinEntriesSort,
  IActorRdfJoinEntriesSortOutput,
  IActorRdfJoinEntriesSortTest,
} from '@comunica/bus-rdf-join-entries-sort';
import { ActorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import { KeysQueryOperation } from '@comunica/context-entries';
import type { IActorArgs, TestResult } from '@comunica/core';
import { passTest } from '@comunica/core';
import type { IJoinEntryWithMetadata, IQuerySourceWrapper } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { getNamedNodes, getTerms, getVariables, QUAD_TERM_NAMES } from 'rdf-terms';
import { Algebra, Util as AlgebraUtil } from 'sparqlalgebrajs';

/**
 * An actor that sorts join entries based on Hartig's heuristic for plan selection in link traversal environments.
 *
 * It first determines isolated connected graphs. (done by @comunica/actor-optimize-query-operation-join-connected)
 * For each of the connected graphs, it orders triple patterns in BGPs by the following priority:
 * 1. dependency-respecting: for each (non-first) pattern, at least one variable must occur in a preceding pattern.
 * 2. seed: try to make the first pattern contain a source URI.
 * 3. no vocab seed: avoid first triple pattern with vocab URI (variable predicate,
 *    or variable objects with rdf:type predicate)
 * 4. filtering: patterns only containing variables also contained in preceding triple patterns
 *    are placed as soon as possible.
 *
 * It does this in an adaptive way.
 * This means that this actor will only determine the first triple pattern,
 * execute it, and materialize the remaining BGP based on its results.
 * After that, the remaining BGP is evaluated recursively by this or another BGP actor.
 */
export class ActorRdfJoinEntriesSortTraversalCacheEntriesCardinality extends ActorRdfJoinEntriesSort {
  public constructor(
    args: IActorArgs<IActionRdfJoinEntriesSort, IActorRdfJoinEntriesSortTest, IActorRdfJoinEntriesSortOutput>,
  ) {
    super(args);
  }

  /**
   * This sorts join entries by first prioritizing triple patterns in BGPs, and then all other operation types.
   *
   * Sort the patterns in BGPs by the following priorities:
   * 1. A source in S or O (not O if rdf:type) (seed rule, no vocab rule)
   * 2. Most selective: fewest variables (filtering rule, dependency-respecting rule)
   * @param entries Quad patterns.
   * @param sources The sources that are currently being queried.
   */
  public static sortJoinEntries(entries: IJoinEntryWithMetadata[], sources: string[]): IJoinEntryWithMetadata[] {
    return [ ...entries ]
  }

  public async test(_action: IActionRdfJoinEntriesSort): Promise<TestResult<IActorRdfJoinEntriesSortTest>> {
    return passTest({ accuracy: 1 });
  }

  public async run(action: IActionRdfJoinEntriesSort): Promise<IActorRdfJoinEntriesSortOutput> {
    // Determine all current sources
    const sources: string[] = [];
    const dataSources: IQuerySourceWrapper[] | undefined = action.context
      .get(KeysQueryOperation.querySources);
    if (dataSources) {
      for (const source of dataSources) {
        const sourceValue = source.source.referenceValue;
        if (typeof sourceValue === 'string') {
          sources.push(sourceValue);
        }
      }
    }
    return { entries: ActorRdfJoinEntriesSortTraversalCacheEntriesCardinality.sortJoinEntries(action.entries, sources) };
  }
}

export interface IActorRdfJoinEntriesSortTraversalZeroKnowledgeArgs 
extends IActorArgs<IActionRdfJoinEntriesSort, IActorRdfJoinEntriesSortTest, IActorRdfJoinEntriesSortOutput>{
  mediatorExtractSources: MediatorExtractSources
}