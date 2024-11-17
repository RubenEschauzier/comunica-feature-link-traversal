import { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import type {
  ILink,
  BindingsStream,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
  QuerySourceReference,
} from '@comunica/types';
import type { Quad } from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import type { Operation, Ask, Update } from 'sparqlalgebrajs/lib/algebra';
import type { ITopologyUpdate } from '../lib/StatisticTraversalTopology';
import { StatisticTraversalTopology } from '../lib/StatisticTraversalTopology';

class MockQuerySource implements IQuerySource {
  public referenceValue: QuerySourceReference;

  public constructor(referenceValue: QuerySourceReference) {
    this.referenceValue = referenceValue;
  }

  public getSelectorShape: (context: IActionContext) => Promise<FragmentSelectorShape>;
  public queryBindings: (operation: Operation, context: IActionContext, options?: IQueryBindingsOptions | undefined)
  => BindingsStream;

  public queryQuads: (operation: Operation, context: IActionContext) => AsyncIterator<Quad>;
  public queryBoolean: (operation: Ask, context: IActionContext) => Promise<boolean>;
  public queryVoid: (operation: Update, context: IActionContext) => Promise<void>;
  public toString: () => string;
}

describe('StatisticTraversalTopology', () => {
  let statisticTraversalTopology: StatisticTraversalTopology;
  let statisticDiscovery: StatisticLinkDiscovery;
  let statisticDereference: StatisticLinkDereference;
  let link1: ILink;
  let link2: ILink;
  let link3: ILink;
  let link4: ILink;

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2021-01-01T00:00:00Z').getTime());

    statisticDiscovery = new StatisticLinkDiscovery();
    statisticDereference = new StatisticLinkDereference();
    statisticTraversalTopology = new StatisticTraversalTopology(statisticDiscovery, statisticDereference);

    link1 = { url: 'url1' };
    link2 = { url: 'url2' };
    link3 = { url: 'url3' };
    link4 = { url: 'url4' };
  });

  describe('An StatisticTraversalTopology instance should', () => {
    let cb: (data: ITopologyUpdate) => void;

    beforeEach(() => {
      cb = jest.fn(() => {});
    });

    it('return false on addEdge when parent and child are equal', () => {
      const copyLink = { ...link1 };
      expect(statisticTraversalTopology.addEdge(link1, copyLink)).toBe(false);
      expect(statisticTraversalTopology.addEdge(link1, link1)).toBe(false);

      testGraphEquality(statisticTraversalTopology, {
        adjacencyListIn: {},
        adjacencyListOut: {},
        edgesInOrder: [],
        edges: new Set(),
        nodeMetadata: {},
        nodeToIndexDict: {},
        indexToNodeDict: {},
        openNodes: [],
        dereferenceOrder: [],
      });
    });

    it('return false on addEdge when edge already exists', () => {
      expect(statisticTraversalTopology.addEdge(link1, link2)).toBe(true);
      expect(statisticTraversalTopology.addEdge(link1, link2)).toBe(false);

      testGraphEquality(statisticTraversalTopology, {
        adjacencyListIn: { 0: [], 1: [ 0 ]},
        adjacencyListOut: { 0: [ 1 ], 1: []},
        edgesInOrder: [[ 0, 1 ]],
        edges: new Set([ JSON.stringify([ 0, 1 ]) ]),
        nodeMetadata: { 0: {
          seed: true,
          dereferenced: true,
          discoverOrder: [ -1 ],
          dereferenceOrder: -1,
        }, 1: {
          seed: false,
          dereferenced: false,
          discoverOrder: [ 0 ],
          dereferenceOrder: Number.NEGATIVE_INFINITY,
        }},
        nodeToIndexDict: { url2: 0, url1: 1 },
        indexToNodeDict: { 0: 'url2', 1: 'url1' },
        openNodes: [ 1 ],
        dereferenceOrder: [ ],
      });
    });

    it('correctly identify multiple seed URLs', () => {
      expect(statisticTraversalTopology.addEdge(link1, link2)).toBe(true);
      expect(statisticTraversalTopology.addEdge(link1, link3)).toBe(true);

      testGraphEquality(statisticTraversalTopology, {
        adjacencyListIn: { 0: [], 1: [ 0, 2 ], 2: []},
        adjacencyListOut: { 0: [ 1 ], 1: [], 2: [ 1 ]},
        edgesInOrder: [[ 0, 1 ], [ 2, 1 ]],
        edges: new Set([ JSON.stringify([ 0, 1 ]), JSON.stringify([ 2, 1 ]) ]),
        nodeMetadata: { 0: {
          seed: true,
          dereferenced: true,
          discoverOrder: [ -1 ],
          dereferenceOrder: -1,
        }, 1: {
          seed: false,
          dereferenced: false,
          discoverOrder: [ 0, 1 ],
          dereferenceOrder: Number.NEGATIVE_INFINITY,
        }, 2: {
          seed: true,
          dereferenced: true,
          discoverOrder: [ -1 ],
          dereferenceOrder: -1,
        }},
        nodeToIndexDict: { url2: 0, url1: 1, url3: 2 },
        indexToNodeDict: { 0: 'url2', 1: 'url1', 2: 'url3' },
        openNodes: [ 1 ],
        dereferenceOrder: [ ],
      });
    });

    it('addEdge function should correctly build topology', () => {
      expect(statisticTraversalTopology.addEdge(link1, link2)).toBe(true);
      expect(statisticTraversalTopology.addEdge(link1, link2)).toBe(false);
      expect(statisticTraversalTopology.addEdge(link1, link3)).toBe(true);
      expect(statisticTraversalTopology.addEdge(link4, link1)).toBe(true);

      testGraphEquality(statisticTraversalTopology, {
        adjacencyListIn: { 0: [], 1: [ 0, 2 ], 2: [], 3: [ 1 ]},
        adjacencyListOut: { 0: [ 1 ], 1: [ 3 ], 2: [ 1 ], 3: []},
        edgesInOrder: [[ 0, 1 ], [ 2, 1 ], [ 1, 3 ]],
        edges: new Set([ JSON.stringify([ 0, 1 ]), JSON.stringify([ 2, 1 ]), JSON.stringify([ 1, 3 ]) ]),
        nodeMetadata: { 0: {
          seed: true,
          dereferenced: true,
          discoverOrder: [ -1 ],
          dereferenceOrder: -1,
        }, 1: {
          seed: false,
          dereferenced: false,
          discoverOrder: [ 0, 1 ],
          dereferenceOrder: Number.NEGATIVE_INFINITY,
        }, 2: {
          seed: true,
          dereferenced: true,
          discoverOrder: [ -1 ],
          dereferenceOrder: -1,
        }, 3: {
          seed: false,
          dereferenced: false,
          discoverOrder: [ 2 ],
          dereferenceOrder: Number.NEGATIVE_INFINITY,
        }},
        nodeToIndexDict: { url2: 0, url1: 1, url3: 2, url4: 3 },
        indexToNodeDict: { 0: 'url2', 1: 'url1', 2: 'url3', 3: 'url4' },
        openNodes: [ 1, 3 ],
        dereferenceOrder: [ ],
      });
    });

    it('not add link metadata to topology metadata', () => {
      expect(statisticTraversalTopology.addEdge(
        { ...link1, metadata: { key: 'value' }},
        link2,
      )).toBe(true);

      testGraphEquality(statisticTraversalTopology, {
        adjacencyListIn: { 0: [], 1: [ 0 ]},
        adjacencyListOut: { 0: [ 1 ], 1: []},
        edgesInOrder: [[ 0, 1 ]],
        edges: new Set([ JSON.stringify([ 0, 1 ]) ]),
        nodeMetadata: { 0: {
          seed: true,
          dereferenced: true,
          discoverOrder: [ -1 ],
          dereferenceOrder: -1,
        }, 1: {
          seed: false,
          dereferenced: false,
          discoverOrder: [ 0 ],
          dereferenceOrder: Number.NEGATIVE_INFINITY,
        }},
        nodeToIndexDict: { url2: 0, url1: 1 },
        indexToNodeDict: { 0: 'url2', 1: 'url1' },
        openNodes: [ 1 ],
        dereferenceOrder: [ ],
      });
    });

    it('emit event on discovery update', () => {
      statisticTraversalTopology.on(cb);
      statisticDiscovery.updateStatistic({ url: 'url1', metadata: { key: 'value' }}, { url: 'url2' });
      expect(cb).toHaveBeenCalledWith(
        {
          updateType: 'discover',
          adjacencyListIn: { 0: [], 1: [ 0 ]},
          adjacencyListOut: { 0: [ 1 ], 1: []},
          edgesInOrder: [[ 0, 1 ]],
          openNodes: [ 1 ],
          nodeToIndexDict: { url2: 0, url1: 1 },
          indexToNodeDict: { 0: 'url2', 1: 'url1' },
          childNode: 1,
          parentNode: 0,
          dereferenceOrder: [],
        },
      );
    });

    it('not emit event on invalid discovery update', () => {
      statisticDiscovery.updateStatistic({ url: 'url1' }, { url: 'url2' });
      statisticTraversalTopology.on(cb);
      statisticDiscovery.updateStatistic({ url: 'url1' }, { url: 'url2' });
      expect(cb).not.toHaveBeenCalled();
    });

    it('should correctly record dereference events', () => {
      statisticTraversalTopology.addEdge(link1, link2);
      expect(statisticTraversalTopology.setDereferenced(link1)).toBe(true);
      testGraphEquality(statisticTraversalTopology, {
        adjacencyListIn: { 0: [], 1: [ 0 ]},
        adjacencyListOut: { 0: [ 1 ], 1: []},
        edgesInOrder: [[ 0, 1 ]],
        edges: new Set([ JSON.stringify([ 0, 1 ]) ]),
        nodeMetadata: { 0: {
          seed: true,
          dereferenced: true,
          discoverOrder: [ -1 ],
          dereferenceOrder: -1,
        }, 1: {
          seed: false,
          dereferenced: true,
          discoverOrder: [ 0 ],
          dereferenceOrder: 0,
        }},
        nodeToIndexDict: { url2: 0, url1: 1 },
        indexToNodeDict: { 0: 'url2', 1: 'url1' },
        openNodes: [],
        dereferenceOrder: [ 1 ],
      });
    });

    it('should gracefully handle multiple dereference events (should not happen)', () => {
      statisticTraversalTopology.addEdge(link1, link2);
      expect(statisticTraversalTopology.setDereferenced(link1)).toBe(true);
      expect(statisticTraversalTopology.setDereferenced(link1)).toBe(false);

      testGraphEquality(statisticTraversalTopology, {
        adjacencyListIn: { 0: [], 1: [ 0 ]},
        adjacencyListOut: { 0: [ 1 ], 1: []},
        edgesInOrder: [[ 0, 1 ]],
        edges: new Set([ JSON.stringify([ 0, 1 ]) ]),
        nodeMetadata: { 0: {
          seed: true,
          dereferenced: true,
          discoverOrder: [ -1 ],
          dereferenceOrder: -1,
        }, 1: {
          seed: false,
          dereferenced: true,
          discoverOrder: [ 0 ],
          dereferenceOrder: 0,
        }},
        nodeToIndexDict: { url2: 0, url1: 1 },
        indexToNodeDict: { 0: 'url2', 1: 'url1' },
        openNodes: [],
        dereferenceOrder: [ 1 ],
      });
    });

    it('emit event on dereference update', () => {
      statisticTraversalTopology.on(cb);
      statisticDiscovery.updateStatistic({ url: 'url1', metadata: { key: 'value' }}, { url: 'url2' });
      expect(cb).toHaveBeenNthCalledWith(1, {
        updateType: 'discover',
        adjacencyListIn: { 0: [], 1: [ 0 ]},
        adjacencyListOut: { 0: [ 1 ], 1: []},
        edgesInOrder: [[ 0, 1 ]],
        openNodes: [ 1 ],
        nodeToIndexDict: { url2: 0, url1: 1 },
        indexToNodeDict: { 0: 'url2', 1: 'url1' },
        childNode: 1,
        parentNode: 0,
        dereferenceOrder: [ ],
      });
      statisticDereference.updateStatistic({ url: 'url1' }, new MockQuerySource('source'));
      expect(cb).toHaveBeenCalledTimes(2);
      expect(cb).toHaveBeenNthCalledWith(2, {
        updateType: 'dereference',
        adjacencyListIn: { 0: [], 1: [ 0 ]},
        adjacencyListOut: { 0: [ 1 ], 1: []},
        edgesInOrder: [[ 0, 1 ]],
        openNodes: [ ],
        nodeToIndexDict: { url2: 0, url1: 1 },
        indexToNodeDict: { 0: 'url2', 1: 'url1' },
        childNode: 1,
        parentNode: 1,
        dereferenceOrder: [ 1 ],
      });
    });
  });

  afterEach(() => {
    jest.useRealTimers();
  });
});

function testGraphEquality(statisticTraversalTopology, expectedValues: expectedTopologyValues) {
  expect(statisticTraversalTopology.adjacencyListIn).toEqual(expectedValues.adjacencyListIn);
  expect(statisticTraversalTopology.adjacencyListOut).toEqual(expectedValues.adjacencyListOut);
  expect(statisticTraversalTopology.edgesInOrder).toEqual(expectedValues.edgesInOrder);
  expect(statisticTraversalTopology.edges).toEqual(expectedValues.edges);
  expect(statisticTraversalTopology.nodeMetadata).toEqual(expectedValues.nodeMetadata);
  expect(statisticTraversalTopology.indexToNodeDict).toEqual(expectedValues.indexToNodeDict);
  expect(statisticTraversalTopology.nodeToIndexDict).toEqual(expectedValues.nodeToIndexDict);
  expect(statisticTraversalTopology.openNodes).toEqual(expectedValues.openNodes);
  expect(statisticTraversalTopology.dereferenceOrder).toEqual(expectedValues.dereferenceOrder);
}

interface expectedTopologyValues {
  adjacencyListIn: Record<number, number[]>;
  adjacencyListOut: Record<number, number[]>;
  edgesInOrder: number[][];
  edges: Set<string>;
  nodeMetadata: Record<number, any>;
  nodeToIndexDict: Record<string, number>;
  indexToNodeDict: Record<number, string>;
  openNodes: number[];
  dereferenceOrder: number[];
}
