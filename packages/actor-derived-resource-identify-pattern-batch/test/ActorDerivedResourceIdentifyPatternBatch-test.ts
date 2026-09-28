// The identify bus loads a module that is only published as ESM, which this Jest setup cannot parse.
// Nothing tested here uses it.
jest.mock('@rdfjs/to-ntriples', () => ({ __esModule: true, default: (): string => '' }));

import { isBatchQuerySource } from '@comunica/bus-derived-resource-identify';
import { ActionContext, Bus } from '@comunica/core';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorDerivedResourceIdentifyPatternBatch } from '../lib/ActorDerivedResourceIdentifyPatternBatch';
import { QuerySourcePatternBatch } from '../lib/QuerySourcePatternBatch';
import '@comunica/utils-jest';

const DF = new DataFactory();
const AF = new AlgebraFactory(DF);

describe('ActorDerivedResourceIdentifyPatternBatch', () => {
  let bus: any;
  let mediatorDereferenceRdf: any;
  let actor: ActorDerivedResourceIdentifyPatternBatch;
  const context = new ActionContext();

  function action(filter: string): any {
    return {
      context,
      derivedResourceUnidentified: {
        baseUrl: 'http://example.org/pod/',
        template: 'resources/resource-patterns',
        selectors: [ 'http://example.org/pod/**/*.nq' ],
        filter,
      },
    };
  }

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    mediatorDereferenceRdf = {
      mediate: jest.fn(async() => ({
        data: new ArrayIterator([ DF.quad(DF.namedNode('ex:s'), DF.namedNode('ex:p'), DF.namedNode('ex:o')) ]),
      })),
    };
    actor = new ActorDerivedResourceIdentifyPatternBatch({ name: 'actor', bus, mediatorDereferenceRdf, maxBatchSize: 2 });
  });

  it('should only test pattern batch filters', async() => {
    await expect(actor.test(action(' patterns\n'))).resolves.toPassTestVoid();
    await expect(actor.test(action('qpf'))).resolves.toFailTest('actor can only identify pattern batch derived resources');
  });

  it('should identify a resource that can batch and answers any single pattern', async() => {
    const { derivedResourceIdentified } = await actor.run(action('patterns'));
    expect(derivedResourceIdentified.iri).toBe('http://example.org/pod/resources/resource-patterns');
    expect(derivedResourceIdentified.selectors).toEqual([ 'http://example.org/pod/**/*.nq' ]);
    expect(isBatchQuerySource(derivedResourceIdentified.querySource)).toBe(true);
    expect(derivedResourceIdentified.derivedResourceSelectorShape).toMatchObject({
      type: 'operation',
      operation: { operationType: 'pattern' },
    });
    expect(derivedResourceIdentified.resourceCoefficients.requests).toBeLessThan(1);
    expect(mediatorDereferenceRdf.mediate).not.toHaveBeenCalled();
  });

  describe('QuerySourcePatternBatch', () => {
    let source: QuerySourcePatternBatch;
    const likes = AF.createPattern(DF.variable('s'), DF.namedNode('http://ex.org/likes'), DF.variable('o'));
    const named = AF.createPattern(
      DF.namedNode('http://ex.org/a'),
      DF.variable('p'),
      DF.literal('x', 'en'),
      DF.namedNode('http://ex.org/g'),
    );

    beforeEach(() => {
      source = new QuerySourcePatternBatch('http://example.org/pod/resources/resource-patterns', 2, mediatorDereferenceRdf, DF);
    });

    it('should put every pattern in the query string, leaving out the default graph', () => {
      const url = new URL(source.getBatchUrl([ likes, named ]));
      expect(url.pathname).toBe('/pod/resources/resource-patterns');
      expect(Object.fromEntries(url.searchParams)).toEqual({
        s0: '?s',
        p0: 'http://ex.org/likes',
        o0: '?o',
        s1: 'http://ex.org/a',
        p1: '?p',
        o1: '"x"@en',
        g1: 'http://ex.org/g',
      });
    });

    it('should request a batch in one request', async() => {
      await expect(source.queryQuadsBatch([ likes, named ], context).toArray()).resolves.toHaveLength(1);
      expect(mediatorDereferenceRdf.mediate).toHaveBeenCalledTimes(1);
      expect(mediatorDereferenceRdf.mediate).toHaveBeenCalledWith({ context, url: source.getBatchUrl([ likes, named ]) });
    });

    it('should answer a single pattern as a batch of one', async() => {
      await expect(source.queryQuads(likes, context).toArray()).resolves.toHaveLength(1);
      expect(mediatorDereferenceRdf.mediate).toHaveBeenCalledWith({ context, url: source.getBatchUrl([ likes ]) });
    });

    it('should reject batches it cannot send', () => {
      expect(() => source.queryQuadsBatch([], context)).toThrow('takes 1 to 2 patterns');
      expect(() => source.queryQuadsBatch([ likes, likes, likes ], context)).toThrow('takes 1 to 2 patterns');
      expect(() => source.queryQuads(AF.createNop(), context)).toThrow('only accepts patterns');
    });
  });
});
