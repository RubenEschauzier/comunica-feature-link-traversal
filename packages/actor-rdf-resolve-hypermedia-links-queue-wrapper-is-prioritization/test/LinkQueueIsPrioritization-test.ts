import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { StatisticIntermediateResults } from '@comunica/statistic-intermediate-results';
import type {
  Bindings,
  PartialResult,
} from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { DataFactory } from 'rdf-data-factory';
import { LinkQueueIsPrioritization } from '../lib/LinkQueueIsPrioritization';
import type * as RDF from '@rdfjs/types';
import { types } from 'sparqlalgebrajs/lib/algebra';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);

describe('LinkQueueIndegreePrioritisation', () => {
  let inner: LinkQueuePriority;
  let queue: LinkQueueIsPrioritization;

  let statisticIntermediateResults: StatisticIntermediateResults;
  beforeEach(() => {
    statisticIntermediateResults = new StatisticIntermediateResults();

    inner = new LinkQueuePriority();
    queue = new LinkQueueIsPrioritization(inner, statisticIntermediateResults);
  });
  describe('LinkQueueIsPrioritization', () => {
    it('should update priorities for new intermediate results', () => {
        queue.push({url: "http://example.com/resource"}, {url: 'v1'});
        queue.push({url: "http://example.com/resource1"}, {url: 'v1'});

        statisticIntermediateResults.updateStatistic({
            type: 'bindings',
            data: BF.fromRecord(
                {'v1': DF.namedNode("http://example.com/resource1")}
            ),
            metadata: { operation: types.PROJECT },
        });
        expect(queue.peek()).toEqual(
            {
                url: "http://example.com/resource1",
                metadata: {
                    priority: 1,
                    index: 0
                }
            });
        expect(queue.pop()).toEqual(
            {
                url: "http://example.com/resource1",
                metadata: {
                    priority: 1,
                    index: 0
                }
            });
    
        expect(queue.priorities).toEqual({
            "http://example.com/resource1": 1
        });
      });
    });

  describe('process results', () => {
    let bindingDefault: Bindings;
    let setPrioritySpy: any;
    beforeEach( () => {
        bindingDefault = BF.fromRecord( {
            v1: DF.namedNode("http://example.com/resource"),
        });
        setPrioritySpy = jest.spyOn(inner, 'setPriority');
    });

    it('should not do anything for Quads', () => {
        const quad: RDF.Quad = DF.quad(DF.namedNode('s1'), DF.namedNode('p1'), DF.namedNode('o1'));
        queue.processIntermediateResult({
            type: 'quads',
            data: quad,
            metadata: {}
        });
        expect(setPrioritySpy).not.toHaveBeenCalled()
    });
    it('should not do anything for literals', () => {
        const bindingLiteral = BF.fromRecord( {v1: DF.literal("invalid")} );
        queue.processIntermediateResult({
            type: 'bindings',
            data: bindingLiteral,
            metadata: {}
        });
        expect(queue.priorities).toEqual({});
        expect(setPrioritySpy).not.toHaveBeenCalled();
    });
    it('should not do anything for blankNodes', () => {
        const bindingLiteral = BF.fromRecord( {v1: DF.blankNode("invalid")} );
        queue.processIntermediateResult({
            type: 'bindings',
            data: bindingLiteral,
            metadata: {}
        });
        expect(queue.priorities).toEqual({});
    });
    it('should strip # sign from URI', () => {
        const bindingQuery = BF.fromRecord( {
            v1: DF.namedNode("http://example.com/resource#38293784"),
        });
        queue.processIntermediateResult({
            type: 'bindings',
            data: bindingQuery,
            metadata: {}
        });
        expect(queue.priorities).toEqual({
            "http://example.com/resource": 1
        });
    });
    it('should strip ? sign from URI', () => {
        const bindingQuery = BF.fromRecord( {
            v1: DF.namedNode("http://example.com/resource?key=value"),
            v2: DF.namedNode("http://example.com/resource?key=value?key2=value2"),
            v3: DF.namedNode("http://example.com/resource?")
        });
        queue.processIntermediateResult({
            type: 'bindings',
            data: bindingQuery,
            metadata: {}
        });
        expect(queue.priorities).toEqual({
            "http://example.com/resource": 3
        });
    });
    it('should update priorities if priority is not yet set', () => {
        queue.processIntermediateResult({
            type: 'bindings',
            data: bindingDefault,
            metadata: {}
        });
        expect(queue.priorities).toEqual({
            "http://example.com/resource": 1
        });
    });
    it('should update priorities if the size of binding > current priority', () => {
        queue.processIntermediateResult({
            type: 'bindings',
            data: bindingDefault,
            metadata: {}
        });
        const bindingBigger = BF.fromRecord( {
            v1: DF.namedNode("http://example.com/resource"),
            v2: DF.namedNode("http://example.com/resource2"),
        });
        queue.processIntermediateResult({
            type: 'bindings',
            data: bindingBigger,
            metadata: {}
        });
        expect(queue.priorities).toEqual({
            "http://example.com/resource": 2,
            "http://example.com/resource2": 2
        });
    });
    it('should not update priorities if the size of binding < current priority', () => {
        const bindingBigger = BF.fromRecord( {
            v1: DF.namedNode("http://example.com/resource"),
            v2: DF.namedNode("http://example.com/resource2"),
        });
        queue.processIntermediateResult({
            type: 'bindings',
            data: bindingBigger,
            metadata: {}
        });
        queue.processIntermediateResult({
            type: 'bindings',
            data: bindingDefault,
            metadata: {}
        });
        expect(queue.priorities).toEqual({
            "http://example.com/resource": 2,
            "http://example.com/resource2": 2
        });
        expect(setPrioritySpy).toHaveBeenCalledTimes(2);
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
})
