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
import type * as RDF from '@rdfjs/types';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { types } from 'sparqlalgebrajs/lib/algebra';
import { LinkQueueIsRcc2Prioritization } from '../lib/LinkQueueIsRcc2Prioritization';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);

describe('LinkQueueIsRcc2Prioritisation', () => {
  let inner: LinkQueuePriority;
  let queue: LinkQueueIsRcc2Prioritization;

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
    queue = new LinkQueueIsRcc2Prioritization(inner, statisticTraversalTopologyRcc, statisticIntermediateResults);
  });

  describe('a dynamic example topology', () => {
    it('should correctly set priorities', (done) => {
      const attributionStream1 = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://A')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://B')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://C')),
      ]);
      const attributionStream2 = new ArrayIterator([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://D')),
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('http://B')),
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
          expect(queue.rcc2Scores).toEqual(
            { 0: 0, 1: 1, 2: 2 },
          );
          expect(queue.peek()).toEqual({ url: 'http://C', metadata: { index: 0, priority: 2 }});
          statisticDiscovery.updateStatistic({ url: 'http://D' }, { url: 'http://B' });
          statisticDiscovery.updateStatistic({ url: 'http://D' }, { url: 'http://C' });
          statisticDiscovery.updateStatistic({ url: 'http://C' }, { url: 'http://D' });
          queue.push({ url: 'http://D' }, { url: 'http://B' });
          queue.push({ url: 'http://D' }, { url: 'http://C' });
          queue.push({ url: 'http://C' }, { url: 'http://D' });
          statisticIntermediateResults.updateStatistic({
            type: 'bindings',
            data: BF.fromRecord({'v1': DF.namedNode('http://D'), 'v2': DF.namedNode('http://A')}),
            metadata: {operation: 'inner'}
          });
          expect(queue.rcc2Scores).toEqual(
            { 0: 2, 1: 1, 2: 3, 3: 6 },
          );
          expect(queue.peek()).toEqual({ url: 'http://D', metadata: { index: 0, priority: 6 }});
          statisticIntermediateResults.updateStatistic({
            type: 'bindings',
            data: bindingWithSource2,
            metadata: { operation: types.PROJECT },
          });
          attributionStream2.on('end', () => {
            try {
              expect(queue.rcc2Scores).toEqual(
                { 0: 2, 1: 1, 2: 5, 3: 10 },
              );
              expect(queue.peek()).toEqual({ url: 'http://D', metadata: { index: 0, priority: 10 }});
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

  // describe('ProcessDiscovery', () => {
  //   let attributionStream: ArrayIterator<Quad>;
  //   let bindingWithSource: Bindings;
  //   let spySetPriority: any;
  //   beforeEach(() => {
  //     attributionStream = new ArrayIterator([
  //       DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('B')),
  //       DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('C')),
  //     ]);
  //     bindingWithSource = BF.fromRecord({ v1: DF.namedNode('v1') })
  //       .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream);
  //     spySetPriority = jest.spyOn(inner, 'setPriority');
  //   });
  //   it('should be called on discovery event', () => {
  //     const processDiscoverySpy = jest.spyOn(queue, 'processDiscovery');
  //     statisticDiscovery.updateStatistic({ url: 'url1' }, { url: 'url2' });
  //     expect(processDiscoverySpy).toHaveBeenCalledTimes(1);
  //   });

  //   it('should initialize parent rcc if parent is seed node', () => {
  //     statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
  //     expect(queue.rcc2Scores[queue.nodeToIndexDict.A]).toBe(0);
  //   });

  //   it('should initialize child rcc if not yet initialized', () => {
  //     statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
  //     expect(queue.rcc2Scores[queue.nodeToIndexDict.B]).toBe(0);
  //   });

  //   it('should not change child rcc if parent rcc = 0', () => {
  //     statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
  //     statisticDiscovery.updateStatistic({ url: 'D' }, { url: 'C' });
  //     statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'D' });
  //     expect(queue.rcc2Scores[queue.nodeToIndexDict.B]).toBe(0);
  //   });
  //   it('should update priority if parent rcc > 0 and new node', (done) => {
  //     statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
  //     statisticIntermediateResults.updateStatistic({
  //       type: 'bindings',
  //       data: bindingWithSource,
  //       metadata: { operation: types.PROJECT },
  //     });
  //     attributionStream.on('end', () => {
  //       try {
  //         statisticDiscovery.updateStatistic({ url: 'C' }, { url: 'B' });
  //         expect(queue.rcc2Scores[queue.nodeToIndexDict.C]).toBe(1);
  //         done();
  //       } catch (error) {
  //         done(error);
  //       }
  //     });
  //   });
  //   it('should update priority if parent rcc > 0 and existing node', (done) => {
  //     statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'C' });
  //     statisticIntermediateResults.updateStatistic({
  //       type: 'bindings',
  //       data: bindingWithSource,
  //       metadata: { operation: types.PROJECT },
  //     });
  //     attributionStream.on('end', () => {
  //       try {
  //         statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
  //         expect(queue.rcc2Scores[queue.nodeToIndexDict.B]).toBe(1);
  //         done();
  //       } catch (error) {
  //         done(error);
  //       }
  //     });
  //   });
  //   it('should update priority if parent rcc > 0 and existing node with is-score > 0', (done) => {
  //     statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'C' });
  //     queue.isScores[1] = 3;
  //     statisticIntermediateResults.updateStatistic({
  //       type: 'bindings',
  //       data: bindingWithSource,
  //       metadata: { operation: types.PROJECT },
  //     });
  //     attributionStream.on('end', () => {
  //       try {
  //         statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
  //         expect(queue.rcc2Scores[queue.nodeToIndexDict.B]).toBe(1);
  //         expect(spySetPriority).toHaveBeenCalledWith('B', 3);
  //         done();
  //       } catch (error) {
  //         done(error);
  //       }
  //     });
  //   });
  // });

  // describe('ProcessResult', () => {
  //   let attributionStream: ArrayIterator<Quad>;
  //   let bindingWithSource: Bindings;

  //   beforeEach(() => {
  //     attributionStream = new ArrayIterator([
  //       DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('B')),
  //       DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('C')),
  //     ]);
  //     bindingWithSource = BF.fromRecord({ v1: DF.namedNode('v1') })
  //       .setContextEntry(KeysMergeBindingsContext.sourcesBindingStream, attributionStream);
  //   });

  //   it('should do nothing if the node has no outgoing edges', (done) => {
  //     const setPrioritySpy = jest.spyOn(inner, 'setPriority');
  //     statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'A' });
  //     statisticDiscovery.updateStatistic({ url: 'C' }, { url: 'A' });
  //     statisticIntermediateResults.updateStatistic({
  //       type: 'bindings',
  //       data: bindingWithSource,
  //       metadata: { operation: types.PROJECT },
  //     });
  //     attributionStream.on('end', () => {
  //       try {
  //         expect(queue.rcc2Scores).toEqual({
  //           0: 0,
  //           1: 0,
  //           2: 0,
  //         });
  //         expect(setPrioritySpy).not.toHaveBeenCalled();
  //         done();
  //       } catch (error) {
  //         done(error);
  //       }
  //     });
  //   });

  //   it('should update all priorities of outgoing neighbours', (done) => {
  //     const setPrioritySpy = jest.spyOn(inner, 'setPriority');
  //     statisticDiscovery.updateStatistic({ url: 'A' }, { url: 'B' });
  //     statisticDiscovery.updateStatistic({ url: 'D' }, { url: 'B' });
  //     statisticDiscovery.updateStatistic({ url: 'D' }, { url: 'C' });
  //     statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'C' });
  //     statisticIntermediateResults.updateStatistic({
  //       type: 'bindings',
  //       data: bindingWithSource,
  //       metadata: { operation: types.PROJECT },
  //     });
  //     attributionStream.on('end', () => {
  //       try {
  //         expect(queue.rcc2Scores).toEqual({
  //           0: 1,
  //           1: 2,
  //           2: 2,
  //           3: 0,
  //         });
  //         expect(setPrioritySpy.mock.calls).toEqual(
  //           [[ 'A', 1 ], [ 'D', 1 ], [ 'D', 2 ], [ 'B', 1 ], [ 'A', 2 ]],
  //         );
  //         done();
  //       } catch (error) {
  //         done(error);
  //       }
  //     });
  //   });
  //   it('should update all priorities of outgoing neighbours with is > 1', (done) => {
  //     const setPrioritySpy = jest.spyOn(inner, 'setPriority');
  //     statisticDiscovery.updateStatistic({ url: 'A' }, { url: 'B' });
  //     statisticDiscovery.updateStatistic({ url: 'D' }, { url: 'B' });
  //     statisticDiscovery.updateStatistic({ url: 'D' }, { url: 'C' });
  //     statisticDiscovery.updateStatistic({ url: 'B' }, { url: 'C' });
  //     queue.isScores[queue.nodeToIndexDict.D] = 2;
  //     statisticIntermediateResults.updateStatistic({
  //       type: 'bindings',
  //       data: bindingWithSource,
  //       metadata: { operation: types.PROJECT },
  //     });
  //     attributionStream.on('end', () => {
  //       try {
  //         expect(queue.rcc2Scores).toEqual({
  //           0: 1,
  //           1: 2,
  //           2: 2,
  //           3: 0,
  //         });
  //         expect(setPrioritySpy.mock.calls).toEqual(
  //           [[ 'A', 1 ], [ 'D', 2 ], [ 'D', 4 ], [ 'B', 1 ], [ 'A', 2 ]],
  //         );
  //         done();
  //       } catch (error) {
  //         done(error);
  //       }
  //     });
  //   });
  // });
  // describe('processIntermediateResult', () => {
  //   let bindingDefault: Bindings;
  //   let setPrioritySpy: any;
  //   beforeEach(() => {
  //     bindingDefault = BF.fromRecord({
  //       v1: DF.namedNode('http://example.com/resource'),
  //     });
  //     statisticDiscovery.updateStatistic(
  //       { url: 'http://example.com/resource' },
  //       { url: 'http://example.com/resource2' },
  //     );
  //     setPrioritySpy = jest.spyOn(inner, 'setPriority');
  //   });
  //   it('should not do anything for non-inner results', () => {
  //     const quad: RDF.Quad = DF.quad(DF.namedNode('s1'), DF.namedNode('p1'), DF.namedNode('o1'));
  //     queue.processIntermediateResult({
  //       type: 'quads',
  //       data: quad,
  //       metadata: { operation: types.PROJECT },
  //     });
  //     expect(setPrioritySpy).not.toHaveBeenCalled();
  //   });
  //   it('should not do anything for Quads', () => {
  //     const quad: RDF.Quad = DF.quad(DF.namedNode('s1'), DF.namedNode('p1'), DF.namedNode('o1'));
  //     queue.processIntermediateResult({
  //       type: 'quads',
  //       data: quad,
  //       metadata: { operation: 'inner' },
  //     });
  //     expect(setPrioritySpy).not.toHaveBeenCalled();
  //   });
  //   it('should not do anything for literals', () => {
  //     const bindingLiteral = BF.fromRecord({ v1: DF.literal('invalid') });
  //     queue.processIntermediateResult({
  //       type: 'bindings',
  //       data: bindingLiteral,
  //       metadata: { operation: 'inner' },
  //     });
  //     expect(queue.isScores).toEqual({});
  //     expect(setPrioritySpy).not.toHaveBeenCalled();
  //   });
  //   it('should not do anything for blankNodes', () => {
  //     const bindingLiteral = BF.fromRecord({ v1: DF.blankNode('invalid') });
  //     queue.processIntermediateResult({
  //       type: 'bindings',
  //       data: bindingLiteral,
  //       metadata: { operation: 'inner' },
  //     });
  //     expect(queue.isScores).toEqual({});
  //   });
  //   it('should strip # sign from URI', () => {
  //     const bindingQuery = BF.fromRecord({
  //       v1: DF.namedNode('http://example.com/resource#38293784'),
  //     });
  //     queue.processIntermediateResult({
  //       type: 'bindings',
  //       data: bindingQuery,
  //       metadata: { operation: 'inner' },
  //     });
  //     expect(queue.isScores).toEqual({
  //       1: 1,
  //     });
  //   });
  //   it('should strip ? sign from URI', () => {
  //     const bindingQuery = BF.fromRecord({
  //       v1: DF.namedNode('http://example.com/resource?key=value'),
  //       v2: DF.namedNode('http://example.com/resource?key=value?key2=value2'),
  //       v3: DF.namedNode('http://example.com/resource?'),
  //     });
  //     queue.processIntermediateResult({
  //       type: 'bindings',
  //       data: bindingQuery,
  //       metadata: { operation: 'inner' },
  //     });
  //     expect(queue.isScores).toEqual({
  //       1: 3,
  //     });
  //   });
  //   it('should update priorities if priority is not yet set', () => {
  //     queue.processIntermediateResult({
  //       type: 'bindings',
  //       data: bindingDefault,
  //       metadata: { operation: 'inner' },
  //     });
  //     expect(queue.isScores).toEqual({
  //       1: 1,
  //     });
  //   });
  //   it('should update priorities if the size of binding > current priority', () => {
  //     queue.processIntermediateResult({
  //       type: 'bindings',
  //       data: bindingDefault,
  //       metadata: { operation: 'inner' },
  //     });
  //     const bindingBigger = BF.fromRecord({
  //       v1: DF.namedNode('http://example.com/resource'),
  //       v2: DF.namedNode('http://example.com/resource2'),
  //     });
  //     queue.processIntermediateResult({
  //       type: 'bindings',
  //       data: bindingBigger,
  //       metadata: { operation: 'inner' },
  //     });
  //     expect(queue.isScores).toEqual({
  //       1: 2,
  //       0: 2,
  //     });
  //   });
  //   it('should not update priorities if the size of binding < current priority', () => {
  //     const bindingBigger = BF.fromRecord({
  //       v1: DF.namedNode('http://example.com/resource'),
  //       v2: DF.namedNode('http://example.com/resource2'),
  //     });
  //     queue.processIntermediateResult({
  //       type: 'bindings',
  //       data: bindingBigger,
  //       metadata: { operation: 'inner' },
  //     });
  //     queue.processIntermediateResult({
  //       type: 'bindings',
  //       data: bindingDefault,
  //       metadata: { operation: 'inner' },
  //     });
  //     expect(queue.isScores).toEqual({
  //       1: 2,
  //       0: 2,
  //     });
  //     expect(setPrioritySpy).toHaveBeenCalledTimes(2);
  //   });
  // });

  // describe('push', () => {
  //   beforeEach(() => {
  //     statisticDiscovery.updateStatistic(
  //       { url: 'url1' },
  //       { url: 'url2' },
  //     );
  //   });
  //   it('should add new links with priority 0', () => {
  //     const pushSpy = jest.spyOn(inner, 'push');
  //     queue.push({ url: 'url1' }, { url: 'url2' });
  //     expect(pushSpy).toHaveBeenCalledWith({ url: 'url1', metadata: { priority: 0, index: 0 }}, { url: 'url2' });
  //   });
  //   it('should add links with rcc > 0 with priority equal to rcc', () => {
  //     const pushSpy = jest.spyOn(inner, 'push');
  //     queue.rcc2Scores[1] = 2;
  //     queue.push({ url: 'url1' }, { url: 'url2' });
  //     expect(pushSpy).toHaveBeenCalledWith({ url: 'url1', metadata: { priority: 2, index: 0 }}, { url: 'url2' });
  //   });
  //   it('should add links with is > 0 with priority equal to is score', () => {
  //     const pushSpy = jest.spyOn(inner, 'push');
  //     queue.isScores[1] = 2;
  //     queue.push({ url: 'url1' }, { url: 'url2' });
  //     expect(pushSpy).toHaveBeenCalledWith({ url: 'url1', metadata: { priority: 2, index: 0 }}, { url: 'url2' });
  //   });
  //   it('should add links with rcc and is > 0 with priority equal to rcc * is', () => {
  //     const pushSpy = jest.spyOn(inner, 'push');
  //     queue.rcc2Scores[1] = 2;
  //     queue.isScores[1] = 2;
  //     queue.push({ url: 'url1' }, { url: 'url2' });
  //     expect(pushSpy).toHaveBeenCalledWith({ url: 'url1', metadata: { priority: 4, index: 0 }}, { url: 'url2' });
  //   });
  //   it('should retain any existing metadata', () => {
  //     const pushSpy = jest.spyOn(inner, 'push');
  //     queue.push({ url: 'url1', metadata: { key: 'value' }}, { url: 'url2' });
  //     expect(pushSpy).toHaveBeenCalledWith(
  //       { url: 'url1', metadata: { priority: 0, index: 0, key: 'value' }},
  //       { url: 'url2' },
  //     );
  //   });
  // });
  // describe('priority', () => {
    
  // });
});
