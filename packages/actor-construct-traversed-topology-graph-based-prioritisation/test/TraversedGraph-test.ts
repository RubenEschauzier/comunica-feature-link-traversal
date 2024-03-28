import type { IActionConstructTraversedTopology } from '@comunica/bus-construct-traversed-topology';
import { ActionContext } from '@comunica/core';
import type { ActorConstructTraversedTopologyUrlToGraph } from '../lib/ActorConstructTraversedTopologyGraphBasedPrioritisation';
import { AdjacencyListGraph } from '../lib/AdjacencyListGraph';
import { assert } from 'console';

describe('TraversedGraph', () => {
  let traversedGraph: AdjacencyListGraph;

  beforeEach(() => {
    traversedGraph = new AdjacencyListGraph();
  });

  describe('An AdjacencyListGraph instance', () => {
    let graph: AdjacencyListGraph

    beforeEach(() => {
      graph = new AdjacencyListGraph();
    });

    it('should add nodes correctly', () => {
      /**
       *          A -> B --|
       *            -> C   ->D
       *            -------|  
       */
      graph.set('A', '', {});
      graph.set('B', 'A', {});
      graph.set('C', 'A', {});
      graph.set('D', 'A', {});
      graph.set('D', 'B', {});
      expect(graph.getGraphDataStructure()[0]).toEqual([[1,2,3],[3],[],[]]);
      expect(graph.getGraphDataStructure()[1]).toEqual([[], [0], [0], [0,1]]);
      expect(graph.getIndexToNode()).toEqual({0: 'A', 1: 'B', 2: 'C', 3: 'D'});
      expect(graph.getNodeToIndex()).toEqual({'A': 0, 'B': 1, 'C': 2, 'D': 3});
      expect(graph.getNumEdges()).toEqual(1);

    });


    it('should not add edges already in graph', () => {
      graph.set('A', '', {});
      graph.set('B', 'A', {});
      graph.set('B', 'A', {});
      graph.set('B', 'A', {});
      expect(graph.getNodeToIndex()).toEqual({'A': 0, 'B': 1})
      expect(graph.getGraphDataStructure()).toEqual([ [[1], []], [[], [0]]]);
    });

    it('should not add self-edges', () => {
      graph.set('A', '', {});
      graph.set('B', 'A', {});
      graph.set('A', 'A', {});
      expect(graph.getGraphDataStructure()).toEqual([ [[1], []], [[], [0]]] );
    });

    it('should correctly track number of times a existing node was used as parent', () => {
      graph.set('A', '', {});
      graph.set('B', 'A', {});
      graph.set('C', 'A', {});
      graph.set('D', 'A', {});
      graph.set('D', 'B', {});
      graph.set('D', 'C', {});
      graph.set('E', 'D', {});
      graph.set('E', 'B', {});
      graph.set('F', 'A', {});
      graph.set('F', 'C', {});
      expect(graph.getNumEdges()).toEqual(4)
    })

    it('should correctly set metadata', () => {
      
    })
  });
});

