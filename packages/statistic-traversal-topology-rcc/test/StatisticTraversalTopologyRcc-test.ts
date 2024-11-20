import { KeysMergeBindingsContext } from '@comunica/context-entries';
import { Bus } from '@comunica/core';
import { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';
import { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import type { ITopologyUpdate } from '@comunica/statistic-traversal-topology';
import { StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';
import type {
  ILink,
} from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { types } from 'sparqlalgebrajs/lib/algebra';
import { StatisticTraversalTopologyRcc } from '../lib/StatisticTraversalTopologyRcc';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);

describe('StatisticTraversalTopologyRcc', () => {
  let bus: any;
  let statisticTraversalTopology: StatisticTraversalTopology;
  let statisticDiscovery: StatisticLinkDiscovery;
  let statisticDereference: StatisticLinkDereference;
  let statisticIntermediateResults: StatisticIntermediateResults;
  let statisticTraversalTopologyRcc: StatisticTraversalTopologyRcc;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2021-01-01T00:00:00Z').getTime());
    statisticDiscovery = new StatisticLinkDiscovery();
    statisticDereference = new StatisticLinkDereference();
    statisticIntermediateResults = new StatisticIntermediateResults();
    statisticTraversalTopology = new StatisticTraversalTopology(statisticDiscovery, statisticDereference);
    statisticTraversalTopologyRcc = new StatisticTraversalTopologyRcc(statisticTraversalTopology, statisticIntermediateResults);
  });

  describe('An StatisticTraversalTopologyRcc instance', () => {
    let cb: (data: ILink) => void;

    beforeEach(() => {
      cb = jest.fn((data: ILink) => {});
    });

    it('(updateStatistic) should initialize nodeResultContributions', () => {
      const update: ITopologyUpdate = {
        updateType: 'discover',
        adjacencyListIn: { 0: [], 1: [ 0 ]},
        adjacencyListOut: { 0: [ 1 ], 1: []},
        edgesInOrder: [[1,0]],
        openNodes: [ 1 ],
        nodeToIndexDict: { url2: 0, url1: 1 },
        indexToNodeDict: { 0: 'url2', 1: 'url1' },
        childNode: 1,
        parentNode: 0,
        dereferenceOrder: []
      };
      expect(statisticTraversalTopologyRcc.updateStatistic(update)).toBe(true);
      expect(statisticTraversalTopologyRcc.nodeResultContribution).toEqual(
        { 0: 0, 1: 0 },
      );
    });
    it('(updateStatistic) only initialize if nodeResultContributions is not yet defined', () => {
      statisticTraversalTopologyRcc.nodeResultContribution = { 0: 5, 1: 3 };
      const update: ITopologyUpdate = {
        updateType: 'discover',
        adjacencyListIn: { 0: [], 1: [ 0 ]},
        adjacencyListOut: { 0: [ 1 ], 1: []},
        edgesInOrder: [[1,0]],
        openNodes: [ 1 ],
        nodeToIndexDict: { url2: 0, url1: 1 },
        indexToNodeDict: { 0: 'url2', 1: 'url1' },
        childNode: 1,
        parentNode: 0,
        dereferenceOrder: []
      };
      expect(statisticTraversalTopologyRcc.updateStatistic(update)).toBe(true);
      expect(statisticTraversalTopologyRcc.nodeResultContribution).toEqual(
        { 0: 5, 1: 3 },
      );
    });
    it('(updateStatistic) should consume the source attribution stream', (done) => {
      const emitSpy = jest.spyOn(statisticTraversalTopologyRcc, 'emit');
      statisticDiscovery.updateStatistic({ url: 'url1' }, { url: 'url2' });
      expect(statisticTraversalTopologyRcc.nodeResultContribution).toEqual({
        0: 0,
        1: 0,
      });
      const attributionStream = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url1')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url2')),
      ]);
      const bindingWithSource = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream);
      statisticTraversalTopologyRcc.updateStatistic({
        updateType: 'result',
        resultType: types.PROJECT,
        binding: bindingWithSource,
      });
      const timeout = setTimeout(() => {
        done.fail(new Error('\'end\' event was not emitted within the expected time'));
      }, 5000);

      attributionStream.on('end', () => {
        clearTimeout(timeout);
        try {
          expect(statisticTraversalTopologyRcc.nodeResultContribution).toEqual({
            0: 1,
            1: 1,
          });
          // Should not emit 'new' events when the source is duplicate.
          expect(emitSpy).toHaveBeenCalledTimes(3);
          done(); // Signal test completion
        } catch (error) {
          done(error);
        }
      });
    });

    it('(updateStatistic) should filter duplicate source attribution values', (done) => {
      const emitSpy = jest.spyOn(statisticTraversalTopologyRcc, 'emit');
      statisticDiscovery.updateStatistic({ url: 'url1' }, { url: 'url2' });
      expect(statisticTraversalTopologyRcc.nodeResultContribution).toEqual({
        0: 0,
        1: 0,
      });
      const attributionStream = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url1')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url2')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url2')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url1')),
      ]);
      const bindingWithSource = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream);
      statisticTraversalTopologyRcc.updateStatistic({
        updateType: 'result',
        resultType: types.PROJECT,
        binding: bindingWithSource,
      });
      const timeout = setTimeout(() => {
        done.fail(new Error('\'end\' event was not emitted within the expected time'));
      }, 5000);

      attributionStream.on('end', () => {
        clearTimeout(timeout);
        try {
          expect(statisticTraversalTopologyRcc.nodeResultContribution).toEqual({
            0: 1,
            1: 1,
          });
          // Should not emit 'new' events when the source is duplicate.
          expect(emitSpy).toHaveBeenCalledTimes(3);
          done(); // Signal test completion
        } catch (error) {
          done(error);
        }
      });
    });

    it('should process an update for project, distinct, and not joins', () => {
      const updateStatisticSpy = jest.spyOn(statisticTraversalTopologyRcc, 'updateStatistic');
      const attributionStream = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url1')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url2')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url2')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url1')),
      ]);
      const bindingWithSource = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream);
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource,
        metadata: { operation: types.PROJECT },
      });
      expect(updateStatisticSpy).toHaveBeenCalledTimes(1);
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource,
        metadata: { operation: types.DISTINCT },
      });
      expect(updateStatisticSpy).toHaveBeenCalledTimes(2);
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource,
        metadata: { operation: 'inner' },
      });
      expect(updateStatisticSpy).toHaveBeenCalledTimes(2);
    });
    it('should only process an update for project, distinct, and joins', () => {
      const updateStatisticSpy = jest.spyOn(statisticTraversalTopologyRcc, 'updateStatistic');
      const attributionStream = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url1')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url2')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url2')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('url1')),
      ]);
      const bindingWithSource = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream);
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource,
        metadata: { operation: types.CONSTRUCT },
      });
      expect(updateStatisticSpy).not.toHaveBeenCalled();
    });
  });

  afterEach(() => {
    jest.useRealTimers();
  });
});
