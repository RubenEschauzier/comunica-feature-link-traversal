import { ActorDerivedResourceDiscoverDeclared, DERIVED_RESOURCES } from '@comunica/actor-derived-resource-discover-declared';
import {
  ActorDerivedResourceDiscoverInterfaceLink,
  VOID_SPARQL_ENDPOINT,
} from '@comunica/actor-derived-resource-discover-interface-link';
import type { ActorDerivedResourceDiscover } from '@comunica/bus-derived-resource-discover';
import { KeysDerivedResourceIdentify, KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { ActionContext, Bus } from '@comunica/core';
import { MediatorCombineArray } from '@comunica/mediator-combine-array';
import type { IActionContext } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'readable-stream';
import { ActorDerivedResourceDiscoverDescribedBy } from '../lib/ActorDerivedResourceDiscoverDescribedBy';
import '@comunica/utils-jest';

const DF = new DataFactory();
const DESCRIBED_BY = 'http://www.w3.org/2007/05/powder-s#describedby';
const POD = 'http://localhost:3000/pods/00000000000000000065/';
const CARD = `${POD}profile/card`;
const META = `${POD}.meta`;

function stream(quads: RDF.Quad[]): RDF.Stream {
  return <any> Readable.from(quads);
}

function dynamicFilter(): any {
  const exact = new Set<string>();
  return {
    exact,
    matchesFilter: (url: string) => exact.has(url),
    addExact: (url: string) => exact.add(url),
  };
}

const card = [ DF.quad(DF.namedNode(`${CARD}#me`), DF.namedNode(DESCRIBED_BY), DF.namedNode(META)) ];

describe('ActorDerivedResourceDiscoverDescribedBy', () => {
  let mediatorDereferenceRdf: any;
  let mediatorDerivedResourceDiscover: any;
  let actor: ActorDerivedResourceDiscoverDescribedBy;
  let filter: any;
  let context: IActionContext;
  const document = (quads: RDF.Quad[]) => ({
    url: CARD,
    describedUrl: CARD,
    kind: <const> 'document',
    metadata: stream(quads),
    context,
  });
  const endpoint = { baseUrl: POD, template: `${POD}sparql`, selectors: [], filter: '' };

  beforeEach(() => {
    filter = dynamicFilter();
    context = new ActionContext({
      [KeysRdfResolveHypermediaLinks.dynamicFilter.name]: filter,
      [KeysDerivedResourceIdentify.derivedResourceDescriptions.name]: new Map(),
    });
    mediatorDereferenceRdf = { mediate: jest.fn(async() => ({ data: stream([]) })) };
    mediatorDerivedResourceDiscover = {
      mediate: jest.fn(async() => ({ discovered: [{ scope: POD, resources: Promise.resolve([ endpoint ]) }]})),
    };
    actor = new ActorDerivedResourceDiscoverDescribedBy({
      name: 'actor',
      bus: new Bus({ name: 'bus' }),
      predicates: [ DESCRIBED_BY ],
      mediatorDereferenceRdf,
      mediatorDerivedResourceDiscover,
    });
  });

  it('tests on traversed documents only, so descriptions are followed one link deep', async() => {
    await expect(actor.test(document([]))).resolves.toPassTestVoid();
    await expect(actor.test({ ...document([]), kind: 'description' })).resolves
      .toFailTest('actor only follows links to descriptions from traversed documents');
  });

  it('discovers what a linked description links to, over the container it describes', async() => {
    const { discovered } = await actor.run(document(card));
    expect(discovered).toHaveLength(1);
    expect(discovered[0].scope).toBe(POD);
    await expect(discovered[0].resources).resolves.toEqual([ endpoint ]);
    expect(mediatorDereferenceRdf.mediate).toHaveBeenCalledWith({ url: META, context });
    expect(mediatorDerivedResourceDiscover.mediate).toHaveBeenCalledWith(expect.objectContaining({
      url: META,
      describedUrl: POD,
      kind: 'description',
    }));
  });

  it('takes a description up once per query, and keeps it off the crawl', async() => {
    await actor.run(document(card));
    expect(filter.exact).toEqual(new Set([ META ]));
    await expect(actor.run(document(card))).resolves.toEqual({ discovered: []});
    expect(mediatorDereferenceRdf.mediate).toHaveBeenCalledTimes(1);
  });

  it('uses a description that was fetched ahead of the seeds', async() => {
    const prefetched = Promise.resolve([ endpoint ]);
    context.getSafe(KeysDerivedResourceIdentify.derivedResourceDescriptions).set(META, prefetched);
    const { discovered } = await actor.run(document(card));
    expect(discovered[0].resources).toBe(prefetched);
    expect(mediatorDereferenceRdf.mediate).not.toHaveBeenCalled();
  });

  it('ignores literals and predicates that are not configured', async() => {
    await expect(actor.run(document([
      DF.quad(DF.namedNode(CARD), DF.namedNode(DESCRIBED_BY), DF.literal(META)),
      DF.quad(DF.namedNode(CARD), DF.namedNode('http://xmlns.com/foaf/0.1/knows'), DF.namedNode(META)),
    ]))).resolves.toEqual({ discovered: []});
  });

  describe('on a bus with the other discover actors', () => {
    let mediator: MediatorCombineArray<ActorDerivedResourceDiscover, any, any, any>;

    beforeEach(() => {
      const bus: any = new Bus({ name: 'bus' });
      mediator = new MediatorCombineArray({ name: 'mediator', bus, fields: [ 'discovered' ], filterErrors: true });
      mediatorDereferenceRdf.mediate = jest.fn(async() => ({
        data: stream([
          DF.quad(DF.namedNode(POD), DF.namedNode(`${DERIVED_RESOURCES}derivedResource`), DF.blankNode('r')),
          DF.quad(DF.blankNode('r'), DF.namedNode(`${DERIVED_RESOURCES}template`), DF.literal('derived/qpf')),
          DF.quad(DF.blankNode('r'), DF.namedNode(`${DERIVED_RESOURCES}selector`), DF.literal(`${POD}**`)),
          DF.quad(DF.blankNode('r'), DF.namedNode(`${DERIVED_RESOURCES}filter`), DF.literal('qpf')),
          DF.quad(DF.namedNode(POD), DF.namedNode(VOID_SPARQL_ENDPOINT), DF.namedNode(`${POD}sparql`)),
        ]),
      }));
      new ActorDerivedResourceDiscoverDescribedBy({
        name: 'described-by',
        bus,
        predicates: [ DESCRIBED_BY ],
        mediatorDereferenceRdf,
        mediatorDerivedResourceDiscover: mediator,
      });
      new ActorDerivedResourceDiscoverDeclared({ name: 'declared', bus, mediatorDereference: <any> {}});
      new ActorDerivedResourceDiscoverInterfaceLink({ name: 'interface-link', bus, predicates: [ VOID_SPARQL_ENDPOINT ]});
    });

    it('discovers the resources of a description through the link of a document', async() => {
      const { discovered } = await mediator.mediate(document([
        ...card,
        // Declarations are not read from documents, as a container includes its description
        DF.quad(DF.namedNode(CARD), DF.namedNode(`${DERIVED_RESOURCES}derivedResource`), DF.blankNode('x')),
      ]));
      expect(discovered.map((group: any) => group.scope)).toEqual([ POD ]);
      await expect(discovered[0].resources).resolves.toEqual([
        { baseUrl: POD, template: 'derived/qpf', selectors: [ `${POD}**` ], filter: 'qpf' },
        {
          baseUrl: POD,
          template: `${POD}sparql`,
          selectors: [ `${POD}**` ],
          filter: '',
          interfaceType: VOID_SPARQL_ENDPOINT,
        },
      ]);
    });
  });
});
