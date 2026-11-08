import { KeysInitQuery } from '@comunica/context-entries';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext, IQuerySource } from '@comunica/types';
import { Algebra, AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { ActorDerivedResourceIdentifySparqlEndpoint } from '../lib/ActorDerivedResourceIdentifySparqlEndpoint';
import { QuerySourceSparqlEndpoint } from '../lib/QuerySourceSparqlEndpoint';
import '@comunica/utils-jest';

// The identify bus imports this ESM-only package, which jest cannot load, for templates this
// actor does not use
jest.mock('@rdfjs/to-ntriples', () => ({ __esModule: true, default: () => '' }));

const DF = new DataFactory();
const AF = new AlgebraFactory(DF);
const VOID_SPARQL_ENDPOINT = 'http://rdfs.org/ns/void#sparqlEndpoint';
const POD = 'http://fedshop-proxy:3000/producer2/';
const ENDPOINT = 'http://fedshop-proxy:34343/producer2/sparql';
const WILDCARD = { type: 'operation', operation: { operationType: 'wildcard' }, joinBindings: true };

describe('ActorDerivedResourceIdentifySparqlEndpoint', () => {
  let bus: any;
  let source: IQuerySource;
  let mediatorQuerySourceIdentifyHypermedia: any;
  let actor: ActorDerivedResourceIdentifySparqlEndpoint;
  let context: IActionContext;

  const endpoint = {
    baseUrl: POD,
    template: ENDPOINT,
    selectors: [ `${POD}**` ],
    filter: '',
    interfaceType: VOID_SPARQL_ENDPOINT,
  };

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    source = <any> {
      getSelectorShape: jest.fn(async() => WILDCARD),
      queryBindings: jest.fn(),
      queryQuads: jest.fn(),
    };
    mediatorQuerySourceIdentifyHypermedia = { mediate: jest.fn(async() => ({ source })) };
    actor = new ActorDerivedResourceIdentifySparqlEndpoint({
      name: 'actor',
      bus,
      mediatorQuerySourceIdentifyHypermedia,
      interfaceTypes: [ VOID_SPARQL_ENDPOINT ],
    });
    context = new ActionContext({
      [KeysInitQuery.dataFactory.name]: DF,
      [KeysInitQuery.queryString.name]: 'SELECT * WHERE { ?s ?p ?o }',
    });
  });

  it('tests an interface linked as SPARQL endpoint', async() => {
    await expect(actor.test({ derivedResourceUnidentified: endpoint, context })).resolves.toPassTestVoid();
  });

  it('does not test a derived resource', async() => {
    await expect(actor.test({
      derivedResourceUnidentified: { baseUrl: POD, template: 'derived/qpf', selectors: [], filter: 'qpf' },
      context,
    })).resolves.toFailTest('actor can only identify interfaces linked as SPARQL endpoint');
  });

  it('does not test another kind of interface', async() => {
    await expect(actor.test({
      derivedResourceUnidentified: { ...endpoint, interfaceType: 'http://rdfs.org/ns/void#uriLookupEndpoint' },
      context,
    })).resolves.toFailTest('actor can only identify interfaces linked as SPARQL endpoint');
  });

  it('identifies the endpoint as the SPARQL source, with its shape', async() => {
    const { derivedResourceIdentified } = await actor.run({ derivedResourceUnidentified: endpoint, context });

    expect(mediatorQuerySourceIdentifyHypermedia.mediate).toHaveBeenCalledWith(expect.objectContaining({
      url: ENDPOINT,
      forceSourceType: 'sparql',
      context,
    }));
    expect(derivedResourceIdentified).toMatchObject({
      iri: ENDPOINT,
      baseUrl: POD,
      selectors: [ `${POD}**` ],
      interfaceType: VOID_SPARQL_ENDPOINT,
      derivedResourceSelectorShape: WILDCARD,
    });
    expect(derivedResourceIdentified.querySource).toBeInstanceOf(QuerySourceSparqlEndpoint);
  });

  describe('its source', () => {
    let querySource: QuerySourceSparqlEndpoint;

    beforeEach(() => {
      querySource = new QuerySourceSparqlEndpoint(ENDPOINT, source, AF, DF);
    });

    it('sends the operation asked for rather than the query string', () => {
      const bgp = AF.createBgp([ AF.createPattern(DF.variable('s'), DF.namedNode('ex:p'), DF.variable('o')) ]);
      querySource.queryBindings(bgp, context);

      const [ operation, sentContext ] = (<jest.Mock> source.queryBindings).mock.calls[0];
      expect(operation).toBe(bgp);
      expect(sentContext.get(KeysInitQuery.queryString)).toBeUndefined();
    });

    it('asks for the quads of a pattern over any graph as a CONSTRUCT over the default graph', () => {
      querySource.queryQuads(
        AF.createPattern(DF.variable('s'), DF.namedNode('ex:p'), DF.variable('o'), DF.variable('g')),
        context,
      );

      const [ operation, sentContext ] = (<jest.Mock> source.queryQuads).mock.calls[0];
      const expected = AF.createPattern(DF.variable('s'), DF.namedNode('ex:p'), DF.variable('o'), DF.defaultGraph());
      expect(operation.type).toBe(Algebra.Types.CONSTRUCT);
      expect(operation.template).toHaveLength(1);
      expect(operation.template[0].equals(expected)).toBe(true);
      expect(operation.input.patterns[0].equals(expected)).toBe(true);
      expect(sentContext.get(KeysInitQuery.queryString)).toBeUndefined();
    });

    it('passes a CONSTRUCT on as it is', () => {
      const pattern = AF.createPattern(DF.variable('s'), DF.namedNode('ex:p'), DF.variable('o'));
      const construct = AF.createConstruct(AF.createBgp([ pattern ]), [ pattern ]);
      querySource.queryQuads(construct, context);

      expect((<jest.Mock> source.queryQuads).mock.calls[0][0]).toBe(construct);
    });
  });
});
