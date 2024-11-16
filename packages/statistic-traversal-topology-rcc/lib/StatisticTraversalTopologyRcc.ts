import { KeysMergeBindingsContext } from '@comunica/context-entries';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { ActionContextKey } from '@comunica/core';
import { StatisticBase } from '@comunica/statistic-base';
import type { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';
import type { ITopologyUpdate, StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';
import type { IStatisticBase, LogicalJoinType, PartialResult } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import { types } from 'sparqlalgebrajs/lib/algebra';

// Import { Bindings } from '@comunica/utils-bindings-factory';

export class StatisticTraversalTopologyRcc extends StatisticBase<TopologyUpdateRccEmit> {
  public key: ActionContextKey<IStatisticBase<TopologyUpdateRccEmit>>;
  /**
   * Metadata of node
   */
  public nodeResultContribution: Record<number, number> = {};
  public nodeToIndexDict: Record<string, number> = {};
  public indexToNodeDict: Record<number, string> = {};

  public constructor(
    statisticTraversalTopology: StatisticTraversalTopology,
    statisticIntermediateResults: StatisticIntermediateResults,
  ) {
    super();
    this.key = KeysStatisticsTraversal.traversalTopologyRcc;

    // Dereference events on the topology are not interesting
    statisticTraversalTopology.on((data: ITopologyUpdate) => {
      if (data.updateType === 'discover') {
        const { updateType, ...topologyData } = data;
        this.updateStatistic({
          updateType,
          ...topologyData,
        });
      }
    });
    // Currently only works for bindings
    statisticIntermediateResults.on((data: PartialResult) => {
      if (data.type === 'bindings' &&
        (data.metadata.operation === types.PROJECT ||
          data.metadata.operation === types.DISTINCT ||
          data.metadata.operation === 'inner')
      ) {
        this.updateStatistic({
          updateType: 'result',
          resultType: data.metadata.operation,
          binding: <Bindings> data.data,
        });
      }
    });
  }

  public updateStatistic(update: ITopologyUpdate | IResultUpdate): boolean {
    if (update.updateType === 'discover') {
      const { updateType, ...topologyData } = update;
      this.nodeToIndexDict = update.nodeToIndexDict;
      // If any of the updated nodes don't have a nodeResultContribution, set to 0
      this.nodeResultContribution[update.childNode] ??= 0;
      this.nodeResultContribution[update.parentNode] ??= 0;

      // Discover event emits the underlying topology
      this.emit({
        updateType: 'discover',
        ...topologyData,
        nodeResultContribution: this.nodeResultContribution,
      });
      return true;
    }

    if (update.updateType === 'result') {
      const sources = update.binding.getContextEntry(KeysMergeBindingsContext.sourcesBindingStream)!;
      const sourceQuadsProcessed = new Set();
      // Sources are streams of provenance quads (including possible duplicates)
      sources.on('data', (data: RDF.BaseQuad) => {
        // Provenance is on object
        const prov = data.object.value;
        // Filter duplicates
        if (!sourceQuadsProcessed.has(prov)) {
          sourceQuadsProcessed.add(prov);
          const sourceId = this.nodeToIndexDict[prov];
          this.nodeResultContribution[sourceId]++;
          this.emit({
            updateType: 'result',
            changedNode: sourceId,
            nodeResultContribution: this.nodeResultContribution,
          });
        }
      });
    }
    return true;
  }
}

export interface ITopologyUpdateRccUpdate extends Omit<ITopologyUpdate, 'updateType'> {
  updateType: 'discover';
  nodeResultContribution: Record<number, number>;
}

export interface ITopologyUpdateRccResult {
  updateType: 'result';
  changedNode: number;
  nodeResultContribution: Record<number, number>;
}

export type TopologyUpdateRccEmit =
  ITopologyUpdateRccUpdate | ITopologyUpdateRccResult;

export interface IResultUpdate {
  updateType: 'result';
  resultType: LogicalJoinType | types;
  binding: Bindings;
}

export interface INodeMetadata {
  seed: boolean;
  dereferenced: boolean;
  discoverOrder: number[];
  dereferenceOrder: number;
}
