import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';
import type { ActionContextKey } from '@comunica/core';
import { StatisticBase } from '@comunica/statistic-base';
import { Bindings } from '@comunica/utils-bindings-factory';
import type { IStatisticBase, PartialResult } from '@comunica/types';
// import { Bindings } from '@comunica/utils-bindings-factory';
import { ITopologyUpdate, StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';
import { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';
import { KeysMergeBindingsContext } from '@comunica/context-entries';
import type * as RDF from '@rdfjs/types';

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
    statisticIntermediateResults: StatisticIntermediateResults
  ) {
    super();
    this.key = KeysStatisticsTraversal.traversalTopologyRcc;

    // Dereference events on the topology are not interesting
    statisticTraversalTopology.on((data: ITopologyUpdate) => {
      if (data.updateType === 'discover'){
        const {updateType, ...topologyData} = data;
        this.updateStatistic({
          updateType: "discover",
          ...topologyData
        });  
      }
    });
    // Currently only works for bindings
    statisticIntermediateResults.on((data: PartialResult) => {
      if (data.type === 'bindings'){
        this.updateStatistic({
          updateType: "result",
          binding: <Bindings> data.data
        })
      }
    });
  }

  public updateStatistic(update: ITopologyUpdate | IResultUpdate): boolean {
    if (update.updateType === 'discover'){
      const { updateType, ...topologyData} = update
      this.nodeToIndexDict = topologyData.nodeToIndexDict;
      // Add this node and its result contribution if its new
      if (!this.nodeResultContribution[update.childNode]){
        this.nodeResultContribution[update.childNode] = 0;
      }
      // Discover event emits the underlying topology
      this.emit({
        updateType: "discover",
        ...topologyData,
        nodeResultContribution: this.nodeResultContribution
      });
      return true;
    }

    if (update.updateType === 'result'){
      const sources = update.binding.getContextEntry(KeysMergeBindingsContext.sourcesBindingStream)!;
      const sourceQuadsProcessed = new Set();
      // Sources are streams of provenance quads (including possible duplicates)
      sources.on('data', (data: RDF.BaseQuad) => {
        // Provenance is on object
        const prov = data.object.value;
        // Filter duplicates
        if(!sourceQuadsProcessed.has(prov)){
          sourceQuadsProcessed.add(prov);
          const sourceId = this.nodeToIndexDict[prov];
          this.nodeResultContribution[sourceId]++;
          this.emit({
            updateType: "result",
            changedNode: sourceId,
            nodeResultContribution: this.nodeResultContribution
          })
        }
      });
    }
    return true;
  }
}


export interface ITopologyUpdateRccUpdate extends Omit<ITopologyUpdate, "updateType"> {
  updateType: "discover",
  nodeResultContribution: Record<number, number>
}

export interface ITopologyUpdateRccResult{
  updateType: "result";
  changedNode: number;
  nodeResultContribution: Record<number, number>;
}

export type TopologyUpdateRccEmit = 
  ITopologyUpdateRccUpdate | ITopologyUpdateRccResult;

export interface IResultUpdate {
  updateType: "result";
  binding: Bindings;
}

export interface INodeMetadata {
  seed: boolean;
  dereferenced: boolean;
  discoverOrder: number[];
  dereferenceOrder: number;
}
