import type { QuerySourceSkolemized } from '@comunica/actor-context-preprocess-query-source-skolemize';
import type { 
  IActionRdfJoin, 
  IActorRdfJoinOutputInner, 
  IActorRdfJoinArgs, 
  MediatorRdfJoin, 
  IActorRdfJoinTestSideData 
} from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import { ActionContextKey, failTest, TestResult } from '@comunica/core';
import { passTestWithSideData } from '@comunica/core';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { IJoinEntry, IQuerySource, IQuerySourceWrapper } from '@comunica/types';
import { getSafeBindings, getSources } from '@comunica/utils-query-operation';
import { Factory } from 'sparqlalgebrajs';
import type { Pattern } from 'sparqlalgebrajs/lib/algebra';
import type { QuerySourceHypermedia } from '@comunica/actor-query-source-identify-hypermedia';
import type { ArrayIndex, IEnumerationOutput } from './IndexBasedJoinSampler';
import { IndexBasedJoinSampler } from './IndexBasedJoinSampler';
import { JoinGraph } from './JoinGraph';
import { JoinOrderEnumerator } from './JoinOrderEnumerator';
import type { JoinPlan } from './JoinPlan';
import { MetadataValidationState } from '@comunica/utils-metadata';

/**
 * A comunica Inner Multi Index Sampling RDF Join Actor.
 */
export class ActorRdfJoinMultiIndexSampling extends ActorRdfJoin {
  public readonly mediatorJoin: MediatorRdfJoin;
  public static readonly FACTORY = new Factory();

  public joinSampler: IndexBasedJoinSampler;
  public estimates: IEnumerationOutput;

  public constructor(args: IActorRdfJoinMultiIndexSampling) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'multi-index-sampling',
      limitEntries: 3,
      limitEntriesMin: true,
      canHandleUndefs: true,
      isLeaf: false,
    });
    this.joinSampler = new IndexBasedJoinSampler(args.budget);
  }

  protected override async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    // Call this only once to get cardinalities, then do dynamic programming to find optimal order, perform joins
    // return joined result + purge cardinalities.
    const sourcesOperations = action.entries.map(x => getSources(x.operation));
    const sources: Record<string, IQuerySource> = {};
    sourcesOperations.map(x => x.map((sourceWrapper: IQuerySourceWrapper) => {
      const sourceString = sourceWrapper.source.toString();
      if (sources[sourceString] === undefined) {
        sources[sourceString] = sourceWrapper.source;
      }
    }));
    const sourceToSample = <QuerySourceHypermedia><unknown>
    (<QuerySourceSkolemized><unknown>sources[Object.keys(sources)[0]]).innerSource;
    const source = (await sourceToSample.sourcesState.get([ ...sourceToSample.sourcesState.keys() ][0])!).source;
    if (!source.sample) {
      throw new Error('Found source that does not support sampling');
    }
    if (!source.countQuads) {
      throw new Error('Found source that does not support quad counting');
    }

    // Join Graph reorders entries in breadth-first style from 0 index of entries. So always use the entries of
    // the join graph to ensure number representations of entries match.
    const joinGraph = new JoinGraph(action.entries);
    joinGraph.constructJoinGraphBFS(joinGraph.getEntries()[0]);

    this.estimates = await this.joinSampler.run(
      joinGraph.getEntries().map(x => <Pattern> x.operation),
      500,
      source.sample.bind(source),
      source.countQuads.bind(source),
    );

    if (this.estimates.maxSizeEstimated === 1){
      console.warn(`${this.name}: Cardinality sampling failed, budget exhausted prematurely.`);
      return {
        result: await this.mediatorJoin.mediate({
          type: action.type,
          entries: action.entries,
          context: action.context.set(BUDGET_EXHAUSTED, true)
        }),
      };
    }

    // Seperate the enumerator from graph object to allow for easier testing
    const enumerator: JoinOrderEnumerator = new JoinOrderEnumerator(
      joinGraph.getAdjencyList(),
      this.estimates,
      joinGraph.getEntries().map(x => x.operation),
    );

    const joinPlan: JoinPlan = enumerator.search();

    const multiJoinOutput = await joinPlan.executePlan(
      joinGraph.getEntries(),
      action,
      this.joinTwoEntries.bind(this),
    );

    // This is a partial join plan, so we must join the remaining entries too
    if (joinPlan.entries.size !== action.entries.length){
      const context = action.context.set(BUDGET_EXHAUSTED, true);
      const leftOverEntries = joinGraph.getEntries().filter((_, i) => !joinPlan.entries.has(i));
      const originalMetadataFn = multiJoinOutput.output.metadata();

      multiJoinOutput.output.metadata = async () => {
        const metadata = await originalMetadataFn;
        metadata.cardinality = { value: joinPlan.estimatedSize, type: "estimate" };
        metadata.state = new MetadataValidationState();
        return metadata;
      }

      leftOverEntries.push(multiJoinOutput);
      return {
        result: await this.mediatorJoin.mediate({
          type: action.type,
          entries: leftOverEntries,
          context: context,
        }),
      };
    }
    
    return {
      result: {
        type: 'bindings',
        bindingsStream: multiJoinOutput.output.bindingsStream,
        metadata: async() => { 
          const metadata = await this.constructResultMetadata(
            action.entries,
            await ActorRdfJoin.getMetadatas(action.entries),
            action.context,
          ) 
          metadata.cardinality = {value: joinPlan.estimatedSize, type: "estimate"}
          return metadata;
        },
      },
    };
  }

  protected async getJoinCoefficients(
    action: IActionRdfJoin,
    sideData: IActorRdfJoinTestSideData,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinTestSideData>> {
    if (action.context.get(BUDGET_EXHAUSTED)){
      return failTest(`${this.name}: budget exhausted for join sampling.`);
    }
    return passTestWithSideData({
      iterations: 1,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: 0,
    }, sideData);
  }

  public async joinTwoEntries(entry1: IJoinEntry, entry2: IJoinEntry, action: IActionRdfJoin): Promise<IJoinEntry> {
    return {
      output: getSafeBindings(await this.mediatorJoin
        .mediate({ type: action.type, entries: [ entry1, entry2 ], context: action.context })),
      operation: ActorRdfJoinMultiIndexSampling.FACTORY
        .createJoin([ entry1.operation, entry2.operation ], false),
    };
  }
}


export interface IActorRdfJoinMultiIndexSampling extends IActorRdfJoinArgs {
  /**
   * A mediator for joining Bindings streams
   */
  mediatorJoin: MediatorRdfJoin;
  /**
   * Maximum number of sampleFn calls during sampling
   * @range {integer}
   * @default {100_000}
   */  
  budget: number;
}

export interface IConstructedIndexes {
  /**
   * N3 store
   */
  store: any;
  /**
   * Created array version of index
   */
  arrayIndexes: Record<string, ArrayIndex>;
}

/**
 * Key that indicates to recursive calls to the join bus that there is no more sampling budget
 * for a given query. So other, non-sampling methods should be used
 */
export const BUDGET_EXHAUSTED = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-join:exhausted',
);