import { Bus } from '@comunica/core';
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
});
