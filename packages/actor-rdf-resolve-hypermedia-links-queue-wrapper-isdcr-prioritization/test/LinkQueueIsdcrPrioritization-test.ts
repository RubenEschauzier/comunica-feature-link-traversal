import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';
import type {
  Bindings,
} from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import { types } from 'sparqlalgebrajs/lib/algebra';
import { LinkQueueIsdcrPrioritization } from '../lib/LinkQueueIsdcrPrioritization';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);

describe('LinkQueueIsdcrPrioritisation', () => {
  let inner: LinkQueuePriority;
  let queue: LinkQueueIsdcrPrioritization;

  let statisticIntermediateResults: StatisticIntermediateResults;
  beforeEach(() => {
    statisticIntermediateResults = new StatisticIntermediateResults();

    inner = new LinkQueuePriority();
    queue = new LinkQueueIsdcrPrioritization(inner, statisticIntermediateResults);
  });
  describe('LinkQueueIsPrioritization', () => {
    it('should update priorities for new intermediate results', () => {
      queue.push({ url: 'http://example.com/resource' }, { url: 'v1' });

      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: BF.fromRecord(
          { v1: DF.namedNode('http://example.com/resource'), v2: DF.namedNode('http://example.com/resource') },
        ),
        metadata: { operation: types.PROJECT },
      });
      queue.push({ url: 'http://example.com/resource1' }, { url: 'http://example.com/resource' });
      expect(queue.priorities).toEqual({
        'http://example.com/resource': 2,
        'http://example.com/resource1': 1,
      });
      expect(queue.pop()).toEqual(
        {
          url: 'http://example.com/resource',
          metadata: {
            priority: 2,
            index: 0,
          },
        },
      );
      expect(queue.peek()).toEqual(
        {
          url: 'http://example.com/resource1',
          metadata: {
            priority: 1,
            index: 0,
          },
        },
      );
    });
  });

  describe('process results', () => {
    let bindingDefault: Bindings;
    let setPrioritySpy: any;
    beforeEach(() => {
      bindingDefault = BF.fromRecord({
        v1: DF.namedNode('http://example.com/resource'),
      });
      setPrioritySpy = jest.spyOn(inner, 'setPriority');
    });

    it('should not do anything for Quads', () => {
      const quad: RDF.Quad = DF.quad(DF.namedNode('s1'), DF.namedNode('p1'), DF.namedNode('o1'));
      queue.processIntermediateResult({
        type: 'quads',
        data: quad,
        metadata: {},
      });
      expect(setPrioritySpy).not.toHaveBeenCalled();
    });
    it('should not do anything for literals', () => {
      const bindingLiteral = BF.fromRecord({ v1: DF.literal('invalid') });
      queue.processIntermediateResult({
        type: 'bindings',
        data: bindingLiteral,
        metadata: {},
      });
      expect(queue.priorities).toEqual({});
      expect(setPrioritySpy).not.toHaveBeenCalled();
    });
    it('should not do anything for blankNodes', () => {
      const bindingLiteral = BF.fromRecord({ v1: DF.blankNode('invalid') });
      queue.processIntermediateResult({
        type: 'bindings',
        data: bindingLiteral,
        metadata: {},
      });
      expect(queue.priorities).toEqual({});
    });
    it('should strip # sign from URI', () => {
      const bindingQuery = BF.fromRecord({
        v1: DF.namedNode('http://example.com/resource#38293784'),
      });
      queue.processIntermediateResult({
        type: 'bindings',
        data: bindingQuery,
        metadata: {},
      });
      expect(queue.priorities).toEqual({
        'http://example.com/resource': 1,
      });
    });
    it('should strip ? sign from URI', () => {
      const bindingQuery = BF.fromRecord({
        v1: DF.namedNode('http://example.com/resource?key=value'),
        v2: DF.namedNode('http://example.com/resource?key=value?key2=value2'),
        v3: DF.namedNode('http://example.com/resource?'),
      });
      queue.processIntermediateResult({
        type: 'bindings',
        data: bindingQuery,
        metadata: {},
      });
      expect(queue.priorities).toEqual({
        'http://example.com/resource': 3,
      });
    });
    it('should update priorities if priority is not yet set', () => {
      queue.processIntermediateResult({
        type: 'bindings',
        data: bindingDefault,
        metadata: {},
      });
      expect(queue.priorities).toEqual({
        'http://example.com/resource': 1,
      });
    });
    it('should update priorities if the size of binding > current priority', () => {
      queue.processIntermediateResult({
        type: 'bindings',
        data: bindingDefault,
        metadata: {},
      });
      const bindingBigger = BF.fromRecord({
        v1: DF.namedNode('http://example.com/resource'),
        v2: DF.namedNode('http://example.com/resource2'),
      });
      queue.processIntermediateResult({
        type: 'bindings',
        data: bindingBigger,
        metadata: {},
      });
      expect(queue.priorities).toEqual({
        'http://example.com/resource': 2,
        'http://example.com/resource2': 2,
      });
    });
    it('should not update priorities if the size of binding < current priority', () => {
      const bindingBigger = BF.fromRecord({
        v1: DF.namedNode('http://example.com/resource'),
        v2: DF.namedNode('http://example.com/resource2'),
      });
      queue.processIntermediateResult({
        type: 'bindings',
        data: bindingBigger,
        metadata: {},
      });
      queue.processIntermediateResult({
        type: 'bindings',
        data: bindingDefault,
        metadata: {},
      });
      expect(queue.priorities).toEqual({
        'http://example.com/resource': 2,
        'http://example.com/resource2': 2,
      });
      expect(setPrioritySpy).toHaveBeenCalledTimes(2);
    });
  });

  describe('push', () => {
    it('should add new links with 0 priority parents with priority -1', () => {
      const pushSpy = jest.spyOn(inner, 'push');
      queue.priorities.url2 = 0;
      queue.push({ url: 'url1' }, { url: 'url2' });
      expect(pushSpy).toHaveBeenCalledWith({ url: 'url1', metadata: { priority: -1, index: 0 }}, { url: 'url2' });
    });
    it('should add new links with undefined priority parents with priority 0', () => {
      const pushSpy = jest.spyOn(inner, 'push');
      queue.push({ url: 'url1' }, { url: 'url2' });
      expect(pushSpy).toHaveBeenCalledWith({ url: 'url1', metadata: { priority: 0, index: 0 }}, { url: 'url2' });
    });
    it('should add new links with priority equal to parent priority - 1', () => {
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: BF.fromRecord(
          { v1: DF.namedNode('http://example.com/resource1'), v2: DF.namedNode('http://example.com/resource1'),
          },
        ),
        metadata: { operation: types.PROJECT },
      });
      const pushSpy = jest.spyOn(inner, 'push');
      queue.push({ url: 'http://example.com/resource' }, { url: 'http://example.com/resource1' });
      expect(queue.priorities['http://example.com/resource']).toBe(1);
    });
    it('should push links involved in bindings with correct priority', () => {
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: BF.fromRecord(
          { v1: DF.namedNode('http://example.com/resource') },
        ),
        metadata: { operation: types.PROJECT },
      });
      statisticIntermediateResults.updateStatistic({
        type: 'bindings',
        data: BF.fromRecord(
          { v1: DF.namedNode('http://example.com/resource1'), v2: DF.namedNode('http://example.com/resource1'), v3: DF.namedNode('http://example.com/resource1') },
        ),
        metadata: { operation: types.PROJECT },
      });
      const pushSpy = jest.spyOn(inner, 'push');
      queue.push({ url: 'http://example.com/resource' }, { url: 'http://example.com/resource1' });
      expect(queue.priorities['http://example.com/resource']).toBe(2);
      expect(pushSpy).toHaveBeenNthCalledWith(1, { url: 'http://example.com/resource', metadata: { priority: 2, index: 0 }}, { url: 'http://example.com/resource1' });
      queue.push({ url: 'http://example.com/resource1' }, { url: 'http://example.com/resource' });
      expect(queue.priorities['http://example.com/resource1']).toBe(3);
      expect(pushSpy).toHaveBeenNthCalledWith(2, { url: 'http://example.com/resource1', metadata: { priority: 3, index: 0 }}, { url: 'http://example.com/resource' });
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
