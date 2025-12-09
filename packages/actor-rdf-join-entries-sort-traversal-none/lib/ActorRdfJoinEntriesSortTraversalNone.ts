import type {
  IActionRdfJoinEntriesSort,
  IActorRdfJoinEntriesSortOutput,
  IActorRdfJoinEntriesSortTest,
} from '@comunica/bus-rdf-join-entries-sort';
import { ActorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import type { IActorArgs, TestResult } from '@comunica/core';
import { passTest } from '@comunica/core';

/**
 * An actor that does no sorting.
 */
export class ActorRdfJoinEntriesSortTraversalNone extends ActorRdfJoinEntriesSort {
  public constructor(
    args: IActorArgs<IActionRdfJoinEntriesSort, IActorRdfJoinEntriesSortTest, IActorRdfJoinEntriesSortOutput>,
  ) {
    super(args);
  }

  public async test(_action: IActionRdfJoinEntriesSort): Promise<TestResult<IActorRdfJoinEntriesSortTest>> {
    return passTest({ accuracy: 1 });
  }

  public async run(action: IActionRdfJoinEntriesSort): Promise<IActorRdfJoinEntriesSortOutput> {
    return { entries: [...action.entries] };
  }
}
