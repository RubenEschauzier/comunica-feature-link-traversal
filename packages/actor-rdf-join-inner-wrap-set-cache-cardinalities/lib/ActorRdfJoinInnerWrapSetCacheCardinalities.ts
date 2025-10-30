import {
  ActorRdfJoin,
} from '@comunica/bus-rdf-join';
import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  MediatorRdfJoin,
  IActorRdfJoinOutputInner,
  IActorRdfJoinTestSideData,
} from '@comunica/bus-rdf-join';
import { KeysCaches, KeysRdfJoin } from '@comunica/context-entries';
import { KeysRdfJoin as KeysRdfJoinLT } from '@comunica/context-entries-link-traversal';
import type { TestResult } from '@comunica/core';
import { failTest, passTestWithSideData } from '@comunica/core';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { IQueryOperationResultBindings, ISourceState, MetadataBindings } from '@comunica/types';
import { Algebra } from 'sparqlalgebrajs';

/**
 * A comunica Inner Multi Adaptive Destroy RDF Join Actor.
 */
export class ActorRdfJoinInnerWrapSetCacheCardinalities extends ActorRdfJoin {
  public readonly mediatorJoin: MediatorRdfJoin;
  public readonly minCacheEntries: number;

  public constructor(args: IActorRdfJoinInnerWrapSetCacheCardinalitiesArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'wrap-set-cache-cardinalities',
    });
  }

  public override async test(
    action: IActionRdfJoin,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinTestSideData>> {
    // This actor should only wrap once
    if (action.context.get(KeysRdfJoinLT.skipSetCacheCardinality)) {
      return failTest('Cached cardinalities have already been used to set join entry metadata.');
    }

    // SHould not do on recursive
    if (action.context.get(KeysRdfJoin.isRecursiveJoin)) {
      return failTest('Cached cardinalities cant be set in recursive join');
    }

    // This actor should not run when any of the operations is not a pattern, as the cache
    // can't easily be used for this.
    if (action.entries.some(entry => entry.operation.type !== Algebra.types.PATTERN)) {
      return failTest('Some entries are not a pattern.');
    }

    // The cache can't be run because the cache does not have sufficient entries to use for
    // estimation
    const storeCache = action.context.get(KeysCaches.storeCache);
    if (!storeCache || storeCache.size < this.minCacheEntries) {
      return failTest('Insufficient number of cache entries to run cache-based cardinality sorting.');
    }
    return super.test(action);
  }

  public override async run(
    action: IActionRdfJoin,
    sideData: IActorRdfJoinTestSideData,
  ): Promise<IQueryOperationResultBindings> {
    return super.run(action, sideData);
  }

  protected override async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const operationToCardinality: Record<string, number> = {};

    // Initialize operation cardinalities
    for (const entry of action.entries) {
      const operationKey: string = this.keyOperation(entry.operation);
      operationToCardinality[operationKey] = 0;
    }

    const sourceCache = action.context.get(KeysCaches.storeCache)!;

    // eslint-disable-next-line unicorn/no-useless-spread
    for (const key of [ ...sourceCache.keys() ]) {
      const sourceState: ISourceState = sourceCache.get(key)!;
      const cachedSource = sourceState.source;

      if ('countQuads' in cachedSource && typeof cachedSource.countQuads === 'function') {
        for (const entry of action.entries) {
          const entryOperationKey = this.keyOperation(entry.operation);

          operationToCardinality[entryOperationKey] += await cachedSource.countQuads(
            entry.operation.subject,
            entry.operation.predicate,
            entry.operation.object,
            entry.operation.graph,
          );
        }
      }
    }
    for (const entry of action.entries) {
      const key = this.keyOperation(entry.operation);
      const originalMetadata = entry.output.metadata;
      async function metadataUpdated(): Promise<MetadataBindings> {
        const metadata = await originalMetadata();
        metadata.cardinality = { type: 'estimate', value: operationToCardinality[key] };
        return metadata;
      }
      entry.output.metadata = metadataUpdated;
    }
    console.log("Set cardinalities in wrap")

    // Run the join mediator but with context entry set
    action.context = action.context.set(KeysRdfJoinLT.skipSetCacheCardinality, true);
    const result = await this.mediatorJoin.mediate(action);
    return { result };
  }

  protected override async getJoinCoefficients(
    _action: IActionRdfJoin,
    sideData: IActorRdfJoinTestSideData,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinTestSideData>> {
    return passTestWithSideData({
      iterations: 0,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: 0,
    }, sideData);
  }

  protected keyOperation(operation: Algebra.Operation): string {
    return JSON.stringify(
      [ operation.subject.value, operation.predicate.value, operation.object.value, operation.graph.value ],
    );
  }
}

export interface IActorRdfJoinInnerWrapSetCacheCardinalitiesArgs extends IActorRdfJoinArgs {
  mediatorJoin: MediatorRdfJoin;
  /**
   * @default {128}
   */
  minCacheEntries: number;
}
