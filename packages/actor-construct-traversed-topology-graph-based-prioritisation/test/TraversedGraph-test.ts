import type { IActionConstructTraversedTopology } from '@comunica/bus-construct-traversed-topology';
import { ActionContext } from '@comunica/core';
import type { ActorConstructTraversedTopologyUrlToGraph } from '../lib/ActorConstructTraversedTopologyGraphBasedPrioritisation';
import { AdjacencyListGraph } from '../lib/AdjacencyListGraph';

describe('TraversedGraph', () => {
  let traversedGraph: AdjacencyListGraph;

  beforeEach(() => {
    traversedGraph = new AdjacencyListGraph();
  });

  describe('An ActorConstructTraversedTopologyUrlToGraph instance', () => {
    let actor: ActorConstructTraversedTopologyUrlToGraph;
    let parentAction: IActionConstructTraversedTopology;

    beforeEach(() => {
      parentAction =
        {
          parentUrl: 'null',
          links: [{ url: 'L1' }],
          metadata: [{ sourceNode: true }],
          context: new ActionContext(),
          setDereferenced: false
        };
    });

    // it('should run with parent node', () => {
    //   traversedGraph.addNode(parentAction.links[0].url, parentAction.parentUrl, parentAction.metadata[0]);
    //   return expect(traversedGraph.getAdjacencyMatrix()).toEqual([[ 1 ]]);
    // });

    // it('should run with non-parent node', () => {
    //   const traversalActionA: IActionConstructTraversedTopology =
    //   {
    //     parentUrl: 'L1',
    //     links: [{ url: 'L2' }, { url: 'L3' }, { url: 'L4' }],
    //     metadata: [{ sourceNode: false }, { sourceNode: false }, { sourceNode: false }],
    //     context: new ActionContext(),
    //     setDereferenced: false
    //   };
    //   traversedGraph.addNode(parentAction.links[0].url, parentAction.parentUrl, parentAction.metadata[0]);
    //   for (let i = 0; i < traversalActionA.links.length; i++) {
    //     traversedGraph.addNode(traversalActionA.links[i].url, traversalActionA.parentUrl, traversalActionA.metadata[i]);
    //   }
    //   return expect(traversedGraph.getAdjacencyMatrix()).toEqual([[ 1, 0, 0, 0 ], [ 1, 1, 0, 0 ], [ 1, 0, 1, 0 ], [ 1, 0, 0, 1 ]]);
    // });

    // it('should correctly store metadata', () => {
    //   const traversalActionB: IActionConstructTraversedTopology = { 
    //     parentUrl: 'null',
    //     links: [{ url: 'L1' }],
    //     metadata: [{ sourceNode: true, testMetaData: 'test' }],
    //     context: new ActionContext(),
    //     setDereferenced: false
    //   };

    //   traversedGraph.addNode(traversalActionB.links[0].url, traversalActionB.parentUrl, traversalActionB.metadata[0]);
    //   return expect(traversedGraph.getMetaDataNode('L1')).toEqual({ sourceNode: true, testMetaData: 'test' });
    // });

    // it('should correctly store node indexes', () => {
    //   const traversalActionA: IActionConstructTraversedTopology =
    //     {
    //       parentUrl: 'L1',
    //       links: [{ url: 'L2' }, { url: 'L3' }, { url: 'L4' }],
    //       metadata: [{ sourceNode: false }, { sourceNode: false }, { sourceNode: false }],
    //       context: new ActionContext(),
    //       setDereferenced: false
    //     };
    //   traversedGraph.addNode(parentAction.links[0].url, parentAction.parentUrl, parentAction.metadata[0]);
    //   for (let i = 0; i < traversalActionA.links.length; i++) {
    //     traversedGraph.addNode(traversalActionA.links[i].url, traversalActionA.parentUrl, traversalActionA.metadata[i]);
    //   }
    //   return expect(traversedGraph.getNodeToIndexes()).toEqual({ L1: 0, L2: 1, L3: 2, L4: 3 });
    // });
  });
});

