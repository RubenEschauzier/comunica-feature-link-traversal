import { ActionContext, Bus } from '@comunica/core';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'readable-stream';
import {
  ActorDerivedResourceDiscoverInterfaceLink,
  VOID_SPARQL_ENDPOINT,
} from '../lib/ActorDerivedResourceDiscoverInterfaceLink';
import '@comunica/utils-jest';

const DF = new DataFactory();
const POD = 'http://fedshop-proxy:3000/producer2/';
const ENDPOINT = 'http://fedshop-proxy:34343/producer2/sparql';

function quad(subject: string, predicate: string, object: string): RDF.Quad {
  return DF.quad(DF.namedNode(subject), DF.namedNode(predicate), DF.namedNode(object));
}

function stream(quads: RDF.Quad[]): RDF.Stream {
  return <any> Readable.from(quads);
}

describe('ActorDerivedResourceDiscoverInterfaceLink', () => {
  let actor: ActorDerivedResourceDiscoverInterfaceLink;
  const context = new ActionContext();

  beforeEach(() => {
    actor = new ActorDerivedResourceDiscoverInterfaceLink({
      name: 'actor',
      bus: new Bus({ name: 'bus' }),
      predicates: [ VOID_SPARQL_ENDPOINT ],
    });
  });

  it('tests on documents and descriptions alike', async() => {
    for (const kind of <const> [ 'document', 'description' ]) {
      await expect(actor.test({ url: POD, describedUrl: POD, metadata: stream([]), kind, context }))
        .resolves.toPassTestVoid();
    }
  });

  it('discovers a SPARQL endpoint linked to the described pod as a resource over the whole pod', async() => {
    const { discovered } = await actor.run({
      url: POD,
      describedUrl: POD,
      kind: 'document',
      context,
      metadata: stream([
        quad(POD, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://www.w3.org/ns/pim/space#Storage'),
        quad(POD, VOID_SPARQL_ENDPOINT, ENDPOINT),
      ]),
    });
    expect(discovered).toHaveLength(1);
    expect(discovered[0].scope).toBe(POD);
    await expect(discovered[0].resources).resolves.toEqual([{
      baseUrl: POD,
      template: ENDPOINT,
      selectors: [ `${POD}**` ],
      filter: '',
      interfaceType: VOID_SPARQL_ENDPOINT,
    }]);
  });

  it('discovers nothing in a document without interface links', async() => {
    await expect(actor.run({
      url: POD,
      describedUrl: POD,
      kind: 'document',
      context,
      metadata: stream([ quad(POD, 'http://www.w3.org/ns/ldp#contains', `${POD}Product1`) ]),
    })).resolves.toEqual({ discovered: []});
  });

  it('reads an endpoint once when it is linked more than once', () => {
    expect(actor.readInterfaces([
      quad(POD, VOID_SPARQL_ENDPOINT, ENDPOINT),
      quad(POD, VOID_SPARQL_ENDPOINT, ENDPOINT),
    ], POD)).toHaveLength(1);
  });

  it('ignores endpoints linked to any other resource than the described one', () => {
    expect(actor.readInterfaces([
      quad('http://fedshop-proxy:3000/producer3/', VOID_SPARQL_ENDPOINT, ENDPOINT),
      quad('http://fedshop-proxy:3000/', VOID_SPARQL_ENDPOINT, ENDPOINT),
      quad(`${POD}Product1`, VOID_SPARQL_ENDPOINT, ENDPOINT),
    ], POD)).toEqual([]);
  });

  it('discovers interfaces by the configured predicates only', async() => {
    const predicate = 'http://example.org/queryInterface';
    actor = new ActorDerivedResourceDiscoverInterfaceLink({
      name: 'actor',
      bus: new Bus({ name: 'bus' }),
      predicates: [ predicate ],
    });
    const { discovered } = await actor.run({
      url: POD,
      describedUrl: POD,
      kind: 'document',
      context,
      metadata: stream([
        quad(POD, VOID_SPARQL_ENDPOINT, ENDPOINT),
        quad(POD, predicate, `${ENDPOINT}2`),
      ]),
    });
    expect(discovered).toHaveLength(1);
    await expect(discovered[0].resources).resolves.toEqual([
      expect.objectContaining({ template: `${ENDPOINT}2`, interfaceType: predicate }),
    ]);
  });

  it('claims no documents for a dataset that is not a container', () => {
    const dataset = `${POD}Product1`;
    expect(actor.readInterfaces([ quad(`${dataset}#it`, VOID_SPARQL_ENDPOINT, ENDPOINT) ], dataset))
      .toEqual([ expect.objectContaining({ baseUrl: `${dataset}#it`, selectors: []}) ]);
  });
});
