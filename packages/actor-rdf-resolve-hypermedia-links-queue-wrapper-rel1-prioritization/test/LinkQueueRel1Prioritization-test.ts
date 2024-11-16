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
import { LinkQueueRel1Prioritization } from '../lib/LinkQueueRel1Prioritization';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);

describe('LinkQueueIndegreePrioritisation', () => {
  let inner: LinkQueuePriority;
  let queue: LinkQueueRel1Prioritization;

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
    queue = new LinkQueueRel1Prioritization(inner, statisticTraversalTopologyRcc);
  });
  describe('an dynamic example topology', () => {
    it('should correctly set priorities', (done) => {
      const attributionStream1 = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('A')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('B')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('C')),
      ]);
      const attributionStream2 = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('D')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('B')),
      ]);
      const bindingWithSource1 = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream1);
      const bindingWithSource2 = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream2);

      statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
      statisticDiscovery.updateStatistic({ url: 'C' }, { url: 'A' });
      statisticDiscovery.updateStatistic({ url: 'C' }, { url: 'B' });
      queue.push({ url: 'B' }, { url: 'A' });
      queue.push({ url: 'C' }, { url: 'A' });
      queue.push({ url: 'C' }, { url: 'B' });
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
          expect(queue.peek()).toEqual({ url: 'C', metadata: { index: 0, priority: 2 }});
          statisticDiscovery.updateStatistic({ url: 'D' }, { url: 'B' });
          statisticDiscovery.updateStatistic({ url: 'D' }, { url: 'C' });
          statisticDiscovery.updateStatistic({ url: 'C' }, { url: 'D' });
          queue.push({ url: 'D' }, { url: 'B' });
          queue.push({ url: 'D' }, { url: 'C' });
          queue.push({ url: 'C' }, { url: 'D' });
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
              expect(queue.peek()).toEqual({ url: 'C', metadata: { index: 0, priority: 3 }});
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
    let attributionStream1: ArrayIterator<Quad>;
    let attributionStream2: ArrayIterator<Quad>;
    let bindingWithSource1: Bindings;
    let bindingWithSource2: Bindings;

    beforeEach(() => {
      attributionStream1 = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('B')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('C')),
      ]);
      attributionStream2 = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('B')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('C')),
      ]);

      bindingWithSource1 = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream1);
      bindingWithSource2 = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream2);
    });
    it('should be called on discovery event', () => {
      const processDiscoverySpy = jest.spyOn(queue, 'processDiscovery');
      statisticDiscovery.updateStatistic({ url: 'url1' }, { url: 'url2' });
      expect(processDiscoverySpy).toHaveBeenCalledTimes(1);
    });

    it('should initialize parent rcc if parent is seed node', () => {
      statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
      expect(queue.priorities[queue.nodeToIndexDict.A]).toBe(0);
    });

    it('should initialize child rcc if not yet initialized', () => {
      statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
      expect(queue.priorities[queue.nodeToIndexDict.B]).toBe(0);
    });

    it('should not change child rcc if parent rcc = 0', () => {
      statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
      statisticDiscovery.updateStatistic({ url: 'D' }, { url: 'C' });
      statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'D' });
      expect(queue.priorities[queue.nodeToIndexDict.B]).toBe(0);
    });
    it('should update priority if parent rcc > 0 and new node', (done) => {
      statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource1,
        metadata: { operation: types.PROJECT },
      });
      attributionStream1.on('end', () => {
        try {
          statisticDiscovery.updateStatistic({ url: 'C' }, { url: 'B' });
          expect(queue.priorities[queue.nodeToIndexDict.C]).toBe(1);
          done();
        } catch (error) {
          done(error);
        }
      });
    });
    it('should update priority if parent rcc > 1 and new node', (done) => {
      statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource1,
        metadata: { operation: types.PROJECT },
      });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource2,
        metadata: { operation: types.PROJECT },
      });
      attributionStream2.on('end', () => {
        try {
          statisticDiscovery.updateStatistic({ url: 'C' }, { url: 'B' });
          // This should be true, but is a safety measure
          expect(attributionStream1.done).toBe(true);
          expect(queue.priorities[queue.nodeToIndexDict.C]).toBe(1);
          done();
        } catch (error) {
          done(error);
        }
      });
    });
    it('should update priority if parent rcc > 0 and existing node', (done) => {
      statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'C' });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource1,
        metadata: { operation: types.PROJECT },
      });
      attributionStream1.on('end', () => {
        try {
          statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
          expect(queue.priorities[queue.nodeToIndexDict.B]).toBe(1);
          done();
        } catch (error) {
          done(error);
        }
      });
    });
    it('should update priority if parent rcc > 1 and existing node', (done) => {
      statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'C' });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource1,
        metadata: { operation: types.PROJECT },
      });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource2,
        metadata: { operation: types.PROJECT },
      });
      attributionStream2.on('end', () => {
        try {
          statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
          expect(attributionStream1.done).toBe(true);
          expect(queue.priorities[queue.nodeToIndexDict.B]).toBe(1);
          done();
        } catch (error) {
          done(error);
        }
      });
    });
  });

  describe('ProcessResult', () => {
    let attributionStream1: ArrayIterator<Quad>;
    let bindingWithSource1: Bindings;
    let attributionStream2: ArrayIterator<Quad>;
    let bindingWithSource2: Bindings;

    beforeEach(() => {
      attributionStream1 = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('B')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('C')),
      ]);
      bindingWithSource1 = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream1);
      attributionStream2 = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('B')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('C')),
      ]);
      bindingWithSource2 = BF.fromRecord({ v1: DF.namedNode('v1') })
        .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream2);
    });

    it('should do nothing if the node has no outgoing edges', (done) => {
      const setPrioritySpy = jest.spyOn(inner, 'setPriority');
      statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
      statisticDiscovery.updateStatistic({ url: 'C' }, { url: 'A' });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource1,
        metadata: { operation: types.PROJECT },
      });
      attributionStream1.on('end', () => {
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
      statisticDiscovery.updateStatistic({ url: 'A' }, { url: 'B' });
      statisticDiscovery.updateStatistic({ url: 'D' }, { url: 'B' });
      statisticDiscovery.updateStatistic({ url: 'D' }, { url: 'C' });
      statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'C' });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource1,
        metadata: { operation: types.PROJECT },
      });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: bindingWithSource2,
        metadata: { operation: types.PROJECT },
      });

      attributionStream2.on('end', () => {
        try {
          expect(attributionStream1.done).toBe(true);
          expect(queue.priorities).toEqual({
            0: 1,
            1: 1,
            2: 2,
            3: 0,
          });
          expect(setPrioritySpy.mock.calls).toEqual(
            [[ 'A', 1 ], [ 'D', 1 ], [ 'D', 2 ], [ 'B', 1 ]],
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
