import { AdjacencyListGraphRcc } from '../lib/AdjacencyListGraphRcc';

describe('TraversedGraph', () => {
  let graph: AdjacencyListGraphRcc;

  beforeEach(() => {
    graph = new AdjacencyListGraphRcc();
  });

  describe('An AdjacencyListGraph instance', () => {

    it('should add nodes correctly', () => {
      graph.set('A', '', {});
      graph.set('B', 'A', {});
      graph.set('C', 'A', {});
      graph.set('D', 'A', {});
      graph.set('D', 'B', {});
      expect(graph.getGraphDataStructure()[0]).toEqual([[1,2,3],[3],[],[]]);
      expect(graph.getGraphDataStructure()[1]).toEqual([[], [0], [0], [0,1]]);
      expect(graph.getIndexToNode()).toEqual({0: 'A', 1: 'B', 2: 'C', 3: 'D'});
      expect(graph.getNodeToIndex()).toEqual({'A': 0, 'B': 1, 'C': 2, 'D': 3});
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

  });
  describe('A static graph to add RCC to', () => {
    beforeEach(() => {
      graph.set('A', '', {});
      graph.set('B', 'A', {});
      graph.set('C', 'A', {});
      graph.set('D', 'A', {});
      graph.set('D', 'B', {});      
    });
    it('should set rcc to 0 by default', () => {
      expect(graph.getMetaData('A')).toBeDefined();
      expect(graph.getMetaData('A')!['rcc']).toBeDefined();
      expect(graph.getMetaData('A')!['rcc']).toEqual(0);
    });

    it('should increase rcc', () => {
      graph.increaseRcc('A', 2);
      expect(graph.getMetaData('A')).toBeDefined();
      expect(graph.getMetaData('A')!['rcc']).toEqual(2);
    });

    it("should throw when trying to set rcc of node that doesn't exist", () => {
      expect(() => { graph.increaseRcc('DoesntExist', 2); }).toThrow("Tried to increase rcc of node not in topology");
    });

    it('should track changed rcc', () => {
      graph.increaseRcc('A', 2);
      graph.increaseRcc('A', 4);
      graph.increaseRcc('C', 3);
      expect(graph.getChangedRccNodes()).toEqual({
        'A': 6,
        'C': 3
      })
    });

    it('should correctly reset tracked rcc', () => {
      graph.increaseRcc('A', 2);
      graph.increaseRcc('C', 3);
      graph.resetChangedRccNodes();
      expect(graph.getChangedRccNodes()).toEqual({});
    });

  });
});

