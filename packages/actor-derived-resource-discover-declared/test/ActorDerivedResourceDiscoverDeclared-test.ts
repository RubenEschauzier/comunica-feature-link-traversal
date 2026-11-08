import { ActionContext, Bus } from '@comunica/core';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'readable-stream';
import { ActorDerivedResourceDiscoverDeclared, DERIVED_RESOURCES } from '../lib/ActorDerivedResourceDiscoverDeclared';
import '@comunica/utils-jest';

const DF = new DataFactory();
const POD = 'http://localhost:3000/pods/00000000000000000065/';

function stream(quads: RDF.Quad[]): RDF.Stream {
  return <any> Readable.from(quads);
}

describe('ActorDerivedResourceDiscoverDeclared', () => {
  let mediatorDereference: any;
  let actor: ActorDerivedResourceDiscoverDeclared;
  const context = new ActionContext();
  const resource = DF.blankNode('r');
  const declaration = [
    DF.quad(DF.namedNode(POD), DF.namedNode(`${DERIVED_RESOURCES}derivedResource`), resource),
    DF.quad(resource, DF.namedNode(`${DERIVED_RESOURCES}template`), DF.literal('derived/qpf')),
    DF.quad(resource, DF.namedNode(`${DERIVED_RESOURCES}selector`), DF.literal(`${POD}**`)),
  ];
  const description = (quads: RDF.Quad[]) => ({
    url: `${POD}.meta`,
    describedUrl: POD,
    kind: <const> 'description',
    metadata: stream(quads),
    context,
  });

  beforeEach(() => {
    mediatorDereference = {
      mediate: jest.fn(async() => ({ data: Readable.from([ Buffer.from('  ?s ?p ?o \n') ]) })),
    };
    actor = new ActorDerivedResourceDiscoverDeclared({ name: 'actor', bus: new Bus({ name: 'bus' }), mediatorDereference });
  });

  it('tests on descriptions only, as a container includes its description', async() => {
    await expect(actor.test(description([]))).resolves.toPassTestVoid();
    await expect(actor.test({ ...description([]), kind: 'document' })).resolves
      .toFailTest('actor only reads the derived resources declared in descriptions');
  });

  it('discovers a declaration whose filter is a literal, over the described container', async() => {
    const { discovered } = await actor.run(description([
      ...declaration,
      DF.quad(resource, DF.namedNode(`${DERIVED_RESOURCES}filter`), DF.literal(' qpf ')),
    ]));
    expect(discovered).toHaveLength(1);
    expect(discovered[0].scope).toBe(POD);
    await expect(discovered[0].resources).resolves
      .toEqual([{ baseUrl: POD, template: 'derived/qpf', selectors: [ `${POD}**` ], filter: 'qpf' }]);
    expect(mediatorDereference.mediate).not.toHaveBeenCalled();
  });

  it('discovers a declaration whose filter is a resource, which it fetches', async() => {
    const { discovered } = await actor.run(description([
      ...declaration,
      DF.quad(resource, DF.namedNode(`${DERIVED_RESOURCES}filter`), DF.namedNode(`${POD}filters/f`)),
    ]));
    await expect(discovered[0].resources).resolves.toEqual([{
      baseUrl: POD,
      template: 'derived/qpf',
      selectors: [ `${POD}**` ],
      filterUri: { url: `${POD}filters/f` },
      filter: '?s ?p ?o',
    }]);
    expect(mediatorDereference.mediate).toHaveBeenCalledWith(expect.objectContaining({ url: `${POD}filters/f` }));
  });

  it('discovers nothing for a declaration without a filter', async() => {
    await expect(actor.run(description(declaration))).resolves.toEqual({ discovered: []});
  });

  it('reads a declaration once when it is declared more than once', () => {
    const filter = DF.quad(resource, DF.namedNode(`${DERIVED_RESOURCES}filter`), DF.literal('qpf'));
    expect(actor.readDeclarations([ ...declaration, declaration[0], filter ], POD)).toHaveLength(1);
  });
});
