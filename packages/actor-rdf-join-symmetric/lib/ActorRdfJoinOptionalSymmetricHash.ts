import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner,
  IActorRdfJoinTestSideData,
} from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { TestResult } from '@comunica/core';
import { passTestWithSideData } from '@comunica/core';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { Bindings, MetadataBindings } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';
import { SymmetricJoinIterator } from './SymmetricJoinIterator';

/**
 * A left join (OPTIONAL) that streams both sides symmetrically. Joined results are produced as soon as
 * both of their items have arrived; a left item that joins with nothing is produced once the right side
 * has ended.
 *
 * Items are keyed on the shared variables that are always bound. A shared variable that can be unbound
 * is compatible with every value, so it is left out of the key and checked when the items are joined;
 * without any key variable, every pair is checked, as in a cartesian product.
 *
 * The nested-loop, hash and bind left joins all emit per left item in order and finish a left item only
 * once its right side has ended; over link traversal that is when traversal ends.
 */
export class ActorRdfJoinOptionalSymmetricHash extends ActorRdfJoin {
  public constructor(args: IActorRdfJoinArgs) {
    super(args, {
      logicalType: 'optional',
      physicalName: 'symmetric-hash',
      limitEntries: 2,
      canHandleUndefs: true,
    });
  }

  /**
   * The shared variables that are bound in every item of both sides.
   */
  public static keyVariables(metadatas: MetadataBindings[]): RDF.Variable[] {
    return ActorRdfJoin.overlappingVariables(metadatas)
      .filter(variable => !variable.canBeUndef)
      .map(variable => variable.variable);
  }

  public async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const variables = ActorRdfJoinOptionalSymmetricHash.keyVariables(metadatas);
    const join = new SymmetricJoinIterator<Bindings>(
      action.entries[0].output.bindingsStream,
      action.entries[1].output.bindingsStream,
      bindings => variables.map(variable => termToString(bindings.get(variable))).join(' '),
      (left, right) => ActorRdfJoin.joinBindings(left, right),
      true,
    );
    return {
      result: {
        type: 'bindings',
        bindingsStream: join,
        metadata: async() => await this.constructResultMetadata(action.entries, metadatas, action.context, {}, true),
      },
    };
  }

  protected async getJoinCoefficients(
    _action: IActionRdfJoin,
    sideData: IActorRdfJoinTestSideData,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinTestSideData>> {
    const { metadatas } = sideData;
    const requestInitialTimes = ActorRdfJoin.getRequestInitialTimes(metadatas);
    const requestItemTimes = ActorRdfJoin.getRequestItemTimes(metadatas);
    const keyed = ActorRdfJoinOptionalSymmetricHash.keyVariables(metadatas).length > 0;
    return passTestWithSideData({
      iterations: keyed ?
        metadatas[0].cardinality.value + metadatas[1].cardinality.value :
        metadatas[0].cardinality.value * metadatas[1].cardinality.value,
      persistedItems: metadatas[0].cardinality.value + metadatas[1].cardinality.value,
      blockingItems: 0,
      requestTime: requestInitialTimes[0] + metadatas[0].cardinality.value * requestItemTimes[0] +
        requestInitialTimes[1] + metadatas[1].cardinality.value * requestItemTimes[1],
    }, sideData);
  }
}
