import { ActorDerivedResourceDiscoverInterfaceLink } from '@comunica/actor-derived-resource-discover-interface-link';
import { ActionContext, Bus, passTestVoid } from '@comunica/core';
import { MediatorCombineArray } from '@comunica/mediator-combine-array';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'readable-stream';
import { ActorExtractLinksSolidDerivedResources } from '../lib/ActorExtractLinksSolidDerivedResources';

// Imported by the derived resource buses, and ESM-only, which jest cannot load
jest.mock('@rdfjs/to-ntriples', () => ({ __esModule: true, default: () => '' }));

const DF = new DataFactory();
const VOID_SPARQL_ENDPOINT = 'http://rdfs.org/ns/void#sparqlEndpoint';
const POD = 'http://fedshop-proxy:3000/producer2/';

/**
 * The metadata stream of a document of which a quad was parsed before the extract links actors got
 * to it, and which another extract links actor reads, so that the quad passes once the stream flows.
 */
function sharedMetadata(): RDF.Stream {
  const metadata = new Readable({ objectMode: true, read: () => {} });
  metadata.push(DF.quad(DF.namedNode(POD), DF.namedNode(VOID_SPARQL_ENDPOINT), DF.namedNode(`${POD}sparql`)));
  metadata.push(null);
  metadata.on('data', () => {});
  return <any> metadata;
}

describe('ActorExtractLinksSolidDerivedResources forwarding metadata to the discover bus', () => {
  const forwardMetadata: (metadata: RDF.Stream) => RDF.Stream =
    (<any> ActorExtractLinksSolidDerivedResources.prototype).forwardMetadata;
  let mediator: MediatorCombineArray<any, any, any, any>;

  beforeEach(() => {
    const bus: any = new Bus({ name: 'bus' });
    mediator = new MediatorCombineArray({ name: 'mediator', bus, fields: [ 'discovered' ], filterErrors: true });
    new ActorDerivedResourceDiscoverInterfaceLink({ name: 'interface-link', bus, predicates: [ VOID_SPARQL_ENDPOINT ]});
    // An actor whose test waits on the event loop, as one doing I/O would, by when the metadata stream
    // has flowed past all actors yet to attach to it
    const slow: any = new ActorDerivedResourceDiscoverInterfaceLink({ name: 'slow', bus, predicates: []});
    slow.test = () => new Promise(resolve => setImmediate(() => resolve(passTestVoid())));
  });

  function discover(metadata: RDF.Stream): Promise<any> {
    return mediator.mediate({ url: POD, describedUrl: POD, kind: 'document', metadata, context: new ActionContext() });
  }

  it('hangs on the shared metadata stream itself, as the actors attach after it ended', async() => {
    const timeout = new Promise(resolve => setTimeout(() => resolve('timeout'), 50));
    await expect(Promise.race([ discover(sharedMetadata()), timeout ])).resolves.toBe('timeout');
  });

  it('discovers on the forwarded metadata stream, which keeps what passed until it is read', async() => {
    const { discovered } = await discover(forwardMetadata(sharedMetadata()));
    expect(discovered.map((group: any) => group.scope)).toEqual([ POD ]);
  });
});
