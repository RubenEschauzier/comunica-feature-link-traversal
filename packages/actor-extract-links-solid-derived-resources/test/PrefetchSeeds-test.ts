import { KeysInitQuery } from '@comunica/context-entries';
import { KeysDerivedResourceIdentify } from '@comunica/context-entries-link-traversal';
import { ActionContext, Bus } from '@comunica/core';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { ActorOptimizeQueryOperationPrefetchDerivedResources } from '../lib/ActorOptimizeQueryOperationPrefetchDerivedResources';

// Imported by the derived resource buses, and ESM-only, which jest cannot load
jest.mock('@rdfjs/to-ntriples', () => ({ __esModule: true, default: () => '' }));

describe('ActorOptimizeQueryOperationPrefetchDerivedResources seeds', () => {
  const actor: any = new ActorOptimizeQueryOperationPrefetchDerivedResources({
    name: 'actor',
    bus: new Bus({ name: 'bus' }),
    mediatorDereferenceRdf: <any> {},
    mediatorDerivedResourceDiscover: <any> {},
  });

  it('takes the untyped sources as seeds, not the ones queried as a type such as a SPARQL endpoint', () => {
    expect(actor.seedUrls([
      'http://fedshop-proxy:3000/taxonomy/ProductType564',
      { value: 'http://fedshop-proxy:3000/vendor0/Offer1' },
      { type: 'sparql', value: 'http://fedshop-proxy:34300/taxonomy/sparql' },
      { type: 'sparql', value: 'http://fedshop-proxy:34301/ratingsite0/sparql' },
      'urn:not:fetched',
    ])).toEqual([
      'http://fedshop-proxy:3000/taxonomy/ProductType564',
      'http://fedshop-proxy:3000/vendor0/Offer1',
    ]);
  });

  describe('run', () => {
    const DF = new DataFactory();
    const AF = new AlgebraFactory(DF);
    const operation = AF.createPattern(DF.variable('s'), DF.namedNode('urn:p'), DF.variable('o'));
    const context = new ActionContext({
      [KeysInitQuery.querySourcesUnidentified.name]: [ 'http://fedshop-proxy:3000/vendor0/Offer1' ],
    });

    function make(prefetchDescriptions?: boolean): { actor: any; dereference: jest.Mock } {
      const dereference = jest.fn(async() => ({ data: { on: jest.fn() }}));
      return {
        actor: new ActorOptimizeQueryOperationPrefetchDerivedResources({
          name: 'actor',
          bus: new Bus({ name: 'bus' }),
          mediatorDereferenceRdf: <any> { mediate: dereference },
          mediatorDerivedResourceDiscover: <any> { mediate: jest.fn(async() => ({ discovered: []})) },
          prefetchDescriptions,
        }),
        dereference,
      };
    }

    it('prefetches the descriptions of the containers above the seeds by default', async() => {
      const { actor, dereference } = make();
      const { context: out } = await actor.run({ operation, context });
      expect([ ...out.get(KeysDerivedResourceIdentify.derivedResourceDescriptions).keys() ]).toEqual([
        'http://fedshop-proxy:3000/vendor0/.meta',
        'http://fedshop-proxy:3000/.meta',
      ]);
      expect(dereference).toHaveBeenCalledTimes(2);
    });

    it('prefetches nothing with prefetchDescriptions false, and still rewrites the query', async() => {
      const { actor, dereference } = make(false);
      const out = await actor.run({ operation, context });
      expect(dereference).not.toHaveBeenCalled();
      expect(out.context.get(KeysDerivedResourceIdentify.derivedResourceDescriptions)).toBeUndefined();
      expect(out.context.get(KeysInitQuery.query)).toBeDefined();
    });
  });
});
