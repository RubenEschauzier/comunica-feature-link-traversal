import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { KeysMergeBindingsContext } from '@comunica/context-entries';
import { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';
import { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import { StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';
import { StatisticTraversalTopologyRcc } from '@comunica/statistic-traversal-topology-rcc';
import type {
  Bindings,
} from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type { Quad } from '@rdfjs/types';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { types } from 'sparqlalgebrajs/lib/algebra';
import { LinkQueueRcc1Prioritization } from '../lib/LinkQueueRcc1Prioritization';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);

describe('LinkQueueRcc1Prioritisation', () => {
  let inner: LinkQueuePriority;
  let queue: LinkQueueRcc1Prioritization;

  let statisticDiscovery: StatisticLinkDiscovery;
  let statisticDereference: StatisticLinkDereference;
  let statisticIntermediateResults: StatisticIntermediateResults;
  let statisticTraversalTopology: StatisticTraversalTopology;
  let statisticTraversalTopologyRcc: StatisticTraversalTopologyRcc;
  beforeEach(() => {
    statisticDiscovery = new StatisticLinkDiscovery();
    statisticDereference = new StatisticLinkDereference();
    statisticTraversalTopology =
      new StatisticTraversalTopology(statisticDiscovery, statisticDereference);
    statisticIntermediateResults = new StatisticIntermediateResults();
    statisticTraversalTopologyRcc = new StatisticTraversalTopologyRcc(
      statisticTraversalTopology,
      statisticIntermediateResults,
    );

    inner = new LinkQueuePriority();
    queue = new LinkQueueRcc1Prioritization(inner, statisticTraversalTopologyRcc);
  });
  describe('an dynamic example topology', () => {
    it('should correctly set priorities', (done) => {
      const attributionStream1 = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://A')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://B')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://C')),
      ]);
      const attributionStream2 = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://D')),
      ]);
      const bindingWithSource1 = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream1);
      const bindingWithSource2 = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream2);

      statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://A' });
      statisticDiscovery.updateStatistic({ url: 'http://C' }, { url: 'http://A' });
      statisticDiscovery.updateStatistic({ url: 'http://C' }, { url: 'http://B' });
      queue.push({ url: 'http://B' }, { url: 'http://A' });
      queue.push({ url: 'http://C' }, { url: 'http://A' });
      queue.push({ url: 'http://C' }, { url: 'http://B' });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource1,
        metadata: { operation: types.PROJECT },
      });
      attributionStream1.on('end', () => {
        try {
          expect(queue.priorities).toEqual(
            { 0: 0, 1: 1, 2: 2 },
          );
          expect(queue.peek()).toEqual({ url: 'http://C', metadata: { index: 0, priority: 2 }});
          statisticDiscovery.updateStatistic({ url: 'http://D' }, { url: 'http://B' });
          statisticDiscovery.updateStatistic({ url: 'http://D' }, { url: 'http://C' });
          statisticDiscovery.updateStatistic({ url: 'http://C' }, { url: 'http://D' });
          queue.push({ url: 'http://D' }, { url: 'http://B' });
          queue.push({ url: 'http://D' }, { url: 'http://C' });
          queue.push({ url: 'http://C' }, { url: 'http://D' });
          expect(queue.priorities).toEqual(
            { 0: 0, 1: 1, 2: 2, 3: 2 },
          );
          statisticIntermediateResults.updateStatistic({
            type: 'bindings',
            data: bindingWithSource2,
            metadata: { operation: types.PROJECT },
          });
          attributionStream2.on('end', () => {
            try {
              expect(queue.priorities).toEqual(
                { 0: 0, 1: 1, 2: 3, 3: 2 },
              );
              expect(queue.peek()).toEqual({ url: 'http://C', metadata: { index: 0, priority: 3 }});
              done();
            } catch (error) {
              done(error);
            }
          });
        } catch (error) {
          done(error);
        }
      });
    });
  });

  describe('ProcessDiscovery', () => {
    let attributionStream: ArrayIterator<Quad>;
    let bindingWithSource: Bindings;
    beforeEach(() => {
      attributionStream = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://B')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://C')),
      ]);
      bindingWithSource = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream);
    });
    it('should be called on discovery event', () => {
      const processDiscoverySpy = jest.spyOn(queue, 'processDiscovery');
      statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://C' });
      expect(processDiscoverySpy).toHaveBeenCalledTimes(1);
    });

    it('should initialize parent rcc if parent is seed node', () => {
      statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://A' });
      expect(queue.priorities[queue.nodeToIndexDict["http://A"]]).toBe(0);
    });

    it('should initialize child rcc if not yet initialized', () => {
      statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://A' });
      expect(queue.priorities[queue.nodeToIndexDict['http://B']]).toBe(0);
    });

    it('should not change child rcc if parent rcc = 0', () => {
      statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://A' });
      statisticDiscovery.updateStatistic({ url: 'http://D' }, { url: 'http://C' });
      statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://D' });
      expect(queue.priorities[queue.nodeToIndexDict['http://B']]).toBe(0);
    });
    it('should update priority if parent rcc > 0 and new node', (done) => {
      statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://A' });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource,
        metadata: { operation: types.PROJECT },
      });
      attributionStream.on('end', () => {
        try {
          statisticDiscovery.updateStatistic({ url: 'http://C' }, { url: 'http://B' });
          expect(queue.priorities[queue.nodeToIndexDict['http://C']]).toBe(1);
          done();
        } catch (error) {
          done(error);
        }
      });
    });
    it('should update priority if parent rcc > 0 and existing node', (done) => {
      statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://C' });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource,
        metadata: { operation: types.PROJECT },
      });
      attributionStream.on('end', () => {
        try {
          statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://A' });
          expect(queue.priorities[queue.nodeToIndexDict['http://B']]).toBe(1);
          done();
        } catch (error) {
          done(error);
        }
      });
    });
  });

  describe('ProcessResult', () => {
    let attributionStream: ArrayIterator<Quad>;
    let bindingWithSource: Bindings;

    beforeEach(() => {
      attributionStream = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://B')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://C')),
      ]);
      bindingWithSource = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream);
    });

    it('should do nothing if the node has no outgoing edges', (done) => {
      const setPrioritySpy = jest.spyOn(inner, 'setPriority');
      statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://A' });
      statisticDiscovery.updateStatistic({ url: 'http://C' }, { url: 'http://A' });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource,
        metadata: { operation: types.PROJECT },
      });
      attributionStream.on('end', () => {
        try {
          expect(queue.priorities).toEqual({
            0: 0,
            1: 0,
            2: 0,
          });
          expect(setPrioritySpy).not.toHaveBeenCalled();
          done();
        } catch (error) {
          done(error);
        }
      });
    });

    it('should update all priorities of outgoing neighbours', (done) => {
      const setPrioritySpy = jest.spyOn(inner, 'setPriority');
      statisticDiscovery.updateStatistic({ url: 'http://A' }, { url: 'http://B' });
      statisticDiscovery.updateStatistic({ url: 'http://D' }, { url: 'http://B' });
      statisticDiscovery.updateStatistic({ url: 'http://D' }, { url: 'http://C' });
      statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://C' });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource,
        metadata: { operation: types.PROJECT },
      });
      attributionStream.on('end', () => {
        try {
          expect(queue.priorities).toEqual({
            0: 1,
            1: 1,
            2: 2,
            3: 0,
          });
          expect(setPrioritySpy.mock.calls).toEqual(
            [[ 'http://A', 1 ], [ 'http://D', 1 ], [ 'http://D', 2 ], [ 'http://B', 1 ]],
          );
          done();
        } catch (error) {
          done(error);
        }
      });
    });
  });

  describe('push', () => {
    it('should add new links with priority 0', () => {
      const pushSpy = jest.spyOn(inner, 'push');
      queue.push({ url: 'url1' }, { url: 'url2' });
      expect(pushSpy).toHaveBeenCalledWith({ url: 'url1', metadata: { priority: 0, index: 0 }}, { url: 'url2' });
    });
    it('should retain any existing metadata', () => {
      const pushSpy = jest.spyOn(inner, 'push');
      queue.push({ url: 'url1', metadata: { key: 'value' }}, { url: 'url2' });
      expect(pushSpy).toHaveBeenCalledWith(
        { url: 'url1', metadata: { priority: 0, index: 0, key: 'value' }},
        { url: 'url2' },
      );
    });
  });
});
