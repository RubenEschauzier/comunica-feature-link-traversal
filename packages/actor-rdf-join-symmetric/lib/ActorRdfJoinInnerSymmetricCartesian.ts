import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner,
  IActorRdfJoinTestSideData,
} from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { TestResult } from '@comunica/core';
import { failTest, passTestWithSideData } from '@comunica/core';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { Bindings } from '@comunica/types';
import { SymmetricJoinIterator } from './SymmetricJoinIterator';

/**
 * An inner join of two entries without shared variables (a cartesian product) that streams both sides
 * symmetrically, so neither side has to end before results are produced.
 *
 * The nested-loop join, the only other join that handles this case, emits per left item in order and
 * finishes a left item only once the right side has ended; over link traversal that is when traversal ends.
 */
export class ActorRdfJoinInnerSymmetricCartesian extends ActorRdfJoin {
  public constructor(args: IActorRdfJoinArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'symmetric-cartesian',
      limitEntries: 2,
      canHandleUndefs: true,
    });
  }

  public async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const join = new SymmetricJoinIterator<Bindings>(
      action.entries[0].output.bindingsStream,
      action.entries[1].output.bindingsStream,
      () => '',
      (left, right) => ActorRdfJoin.joinBindings(left, right),
      false,
    );
    return {
      result: {
        type: 'bindings',
        bindingsStream: join,
        metadata: async() => await this.constructResultMetadata(action.entries, metadatas, action.context),
      },
    };
  }

  protected async getJoinCoefficients(
    _action: IActionRdfJoin,
    sideData: IActorRdfJoinTestSideData,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinTestSideData>> {
    const { metadatas } = sideData;
    if (ActorRdfJoin.overlappingVariables(metadatas).length > 0) {
      return failTest(`${this.name} only joins entries without shared variables`);
    }
    const requestInitialTimes = ActorRdfJoin.getRequestInitialTimes(metadatas);
    const requestItemTimes = ActorRdfJoin.getRequestItemTimes(metadatas);
    return passTestWithSideData({
      iterations: metadatas[0].cardinality.value * metadatas[1].cardinality.value,
      persistedItems: metadatas[0].cardinality.value + metadatas[1].cardinality.value,
      blockingItems: 0,
      requestTime: requestInitialTimes[0] + metadatas[0].cardinality.value * requestItemTimes[0] +
        requestInitialTimes[1] + metadatas[1].cardinality.value * requestItemTimes[1],
    }, sideData);
  }
}
