import { ActorRdfJoinEntriesSortCardinality } from '@comunica/actor-rdf-join-entries-sort-cardinality';
import type {
  IActionRdfJoinEntriesSort,
  IActorRdfJoinEntriesSortOutput,
  IActorRdfJoinEntriesSortTest,
} from '@comunica/bus-rdf-join-entries-sort';
import { KeysRdfJoin } from '@comunica/context-entries';
import * as CELT from '@comunica/context-entries-link-traversal';
import type { IActorArgs, TestResult } from '@comunica/core';
import { failTest, passTest } from '@comunica/core';

/**
 */
export class ActorRdfJoinEntriesSortTraversalCacheEntriesCardinality extends ActorRdfJoinEntriesSortCardinality {
  protected readonly minCacheEntries: number;
  public constructor(
    args: IActorRdfJoinEntriesSortTraversalCacheEntriesCardinality,
  ) {
    super(args);
  }

  public override async test(action: IActionRdfJoinEntriesSort): Promise<TestResult<IActorRdfJoinEntriesSortTest>> {
    const cardinalitySet = action.context.get(CELT.KeysRdfJoin.skipSetCacheCardinality);
    const isRecursive = action.context.get(KeysRdfJoin.isRecursiveJoin);
    if (!cardinalitySet || isRecursive) {
      return failTest('Wrapping join actor has not set cache cardinalities.');
    }
    console.log("Sorting!");
    return passTest({ accuracy: 1 });
  }
}

export interface IActorRdfJoinEntriesSortTraversalCacheEntriesCardinality
  extends IActorArgs<IActionRdfJoinEntriesSort, IActorRdfJoinEntriesSortTest, IActorRdfJoinEntriesSortOutput> {
}
