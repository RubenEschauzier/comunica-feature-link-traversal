import type { IActionConstructTraversedTopology } from '@comunica/bus-construct-traversed-topology';
import { ActionContext } from '@comunica/core';
import type { ActorConstructTraversedTopologyUrlToGraph } from '../lib/ActorConstructTraversedTopologyUrlToGraph';
import { TraversedGraph } from '../lib/TraversedGraph';

describe('TraversedGraph', () => {
  let traversedGraph: TraversedGraph;

  beforeEach(() => {
    traversedGraph = new TraversedGraph();
  });

  describe('An ActorConstructTraversedTopologyUrlToGraph instance', () => {
    let actor: ActorConstructTraversedTopologyUrlToGraph;
    let parentAction: IActionConstructTraversedTopology;

    beforeEach(() => {
      parentAction =
        {
          parentUrl: 'null',
          foundLinks: [{ url: 'L1' }],
          metadata: [{ sourceNode: true }],
          context: new ActionContext(),
        };
    });

    it('should run with parent node', () => {
      traversedGraph.addNode(parentAction.foundLinks[0].url, parentAction.parentUrl, parentAction.metadata[0]);
      return expect(traversedGraph.getAdjacencyMatrix()).toEqual([[ 1 ]]);
    });

    it('should run with non-parent node', () => {
      const traversalActionA: IActionConstructTraversedTopology =
      {
        parentUrl: 'L1',
        foundLinks: [{ url: 'L2' }, { url: 'L3' }, { url: 'L4' }],
        metadata: [{ sourceNode: false }, { sourceNode: false }, { sourceNode: false }],
        context: new ActionContext(),
      };
      traversedGraph.addNode(parentAction.foundLinks[0].url, parentAction.parentUrl, parentAction.metadata[0]);
      for (let i = 0; i < traversalActionA.foundLinks.length; i++) {
        traversedGraph.addNode(traversalActionA.foundLinks[i].url, traversalActionA.parentUrl, traversalActionA.metadata[i]);
      }
      return expect(traversedGraph.getAdjacencyMatrix()).toEqual([[ 1, 0, 0, 0 ], [ 1, 1, 0, 0 ], [ 1, 0, 1, 0 ], [ 1, 0, 0, 1 ]]);
    });

    it('should correctly store metadata', () => {
      const traversalActionB: IActionConstructTraversedTopology = { parentUrl: 'null',
        foundLinks: [{ url: 'L1' }],
        metadata: [{ sourceNode: true, testMetaData: 'test' }],
        context: new ActionContext() };

      traversedGraph.addNode(traversalActionB.foundLinks[0].url, traversalActionB.parentUrl, traversalActionB.metadata[0]);
      return expect(traversedGraph.getMetaDataNode('L1')).toEqual({ sourceNode: true, testMetaData: 'test' });
    });

    it('should correctly store node indexes', () => {
      const traversalActionA: IActionConstructTraversedTopology =
        {
          parentUrl: 'L1',
          foundLinks: [{ url: 'L2' }, { url: 'L3' }, { url: 'L4' }],
          metadata: [{ sourceNode: false }, { sourceNode: false }, { sourceNode: false }],
          context: new ActionContext(),
        };
      traversedGraph.addNode(parentAction.foundLinks[0].url, parentAction.parentUrl, parentAction.metadata[0]);
      for (let i = 0; i < traversalActionA.foundLinks.length; i++) {
        traversedGraph.addNode(traversalActionA.foundLinks[i].url, traversalActionA.parentUrl, traversalActionA.metadata[i]);
      }
      return expect(traversedGraph.getNodeToIndexes()).toEqual({ L1: 0, L2: 1, L3: 2, L4: 3 });
    });
  });
});

