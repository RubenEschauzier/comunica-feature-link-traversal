import { KeysDerivedResourceIdentify, KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import { Readable } from 'readable-stream';
import { ActorDerivedResourceDiscoverWalkUp } from '../lib/ActorDerivedResourceDiscoverWalkUp';
import '@comunica/utils-jest';

const BASE = 'http://fedshop-proxy:3000/';
const POD = `${BASE}producer2/`;

describe('ActorDerivedResourceDiscoverWalkUp', () => {
  let mediatorDereferenceRdf: any;
  let mediatorDerivedResourceDiscover: any;
  let exact: Set<string>;
  let context: IActionContext;
  const endpoint = { baseUrl: POD, template: 'http://fedshop-proxy:34343/producer2/sparql', selectors: [], filter: '' };
  const document = (url: string) => ({
    url,
    describedUrl: url,
    kind: <const> 'document',
    metadata: <any> Readable.from([]),
    context,
  });
  const actor = (maxDepth: number) => new ActorDerivedResourceDiscoverWalkUp({
    name: 'actor',
    bus: new Bus({ name: 'bus' }),
    maxDepth,
    mediatorDereferenceRdf,
    mediatorDerivedResourceDiscover,
  });

  beforeEach(() => {
    exact = new Set();
    context = new ActionContext({
      [KeysRdfResolveHypermediaLinks.dynamicFilter.name]: {
        matchesFilter: (url: string) => exact.has(url),
        addExact: (url: string) => exact.add(url),
      },
      [KeysDerivedResourceIdentify.derivedResourceDescriptions.name]: new Map(),
    });
    mediatorDereferenceRdf = { mediate: jest.fn(async() => ({ data: Readable.from([]) })) };
    mediatorDerivedResourceDiscover = {
      mediate: jest.fn(async() => ({ discovered: [{ scope: POD, resources: Promise.resolve([ endpoint ]) }]})),
    };
  });

  it('tests on traversed documents only', async() => {
    await expect(actor(1).test(document(`${POD}Product1`))).resolves.toPassTestVoid();
    await expect(actor(1).test({ ...document(`${POD}.meta`), kind: 'description' })).resolves
      .toFailTest('actor only walks up from traversed documents');
  });

  it('lists the descriptions of the containers a document lies in, down to the depth', () => {
    expect(actor(1).containerDescriptions(`${POD}Product1#it`)).toEqual([ `${POD}.meta` ]);
    expect(actor(1).containerDescriptions(`${POD}listings/Product1?x=1`)).toEqual([ `${POD}.meta` ]);
    expect(actor(2).containerDescriptions(`${POD}listings/Product1`))
      .toEqual([ `${POD}.meta`, `${POD}listings/.meta` ]);
  });

  it('does not list a container itself, nor the server root', () => {
    expect(actor(1).containerDescriptions(POD)).toEqual([]);
    expect(actor(2).containerDescriptions(`${POD}listings/`)).toEqual([ `${POD}.meta` ]);
    expect(actor(1).containerDescriptions(`${BASE}document`)).toEqual([]);
    expect(actor(1).containerDescriptions('not a url')).toEqual([]);
  });

  it('discovers what the description of the pod links to, over the pod', async() => {
    const { discovered } = await actor(1).run(document(`${POD}Product1`));
    expect(discovered.map(group => group.scope)).toEqual([ POD ]);
    await expect(discovered[0].resources).resolves.toEqual([ endpoint ]);
    expect(mediatorDereferenceRdf.mediate).toHaveBeenCalledWith({ url: `${POD}.meta`, context });
    expect(mediatorDerivedResourceDiscover.mediate).toHaveBeenCalledWith(expect.objectContaining({
      url: `${POD}.meta`,
      describedUrl: POD,
      kind: 'description',
    }));
  });

  it('probes a pod once per query, whichever of its documents leads to it', async() => {
    await actor(1).run(document(`${POD}Product1`));
    await expect(actor(1).run(document(`${POD}Product2`))).resolves.toEqual({ discovered: []});
    expect(mediatorDereferenceRdf.mediate).toHaveBeenCalledTimes(1);
    expect(exact).toEqual(new Set([ `${POD}.meta` ]));
  });

  it('discovers nothing over a container whose description links to nothing', async() => {
    mediatorDerivedResourceDiscover.mediate.mockResolvedValue({ discovered: []});
    const { discovered } = await actor(2).run(document(`${POD}products/Product1`));
    expect(discovered.map(group => group.scope)).toEqual([ POD, `${POD}products/` ]);
    for (const group of discovered) {
      await expect(group.resources).resolves.toEqual([]);
    }
  });

  it('discovers nothing over a container whose description cannot be read', async() => {
    mediatorDereferenceRdf.mediate.mockRejectedValue(new Error('Could not retrieve (HTTP status 404)'));
    const { discovered } = await actor(1).run(document(`${POD}Product1`));
    await expect(discovered[0].resources).resolves.toEqual([]);
    expect(exact).toEqual(new Set([ `${POD}.meta` ]));
  });

  it('uses a description that was fetched ahead of the seeds', async() => {
    const prefetched = Promise.resolve([ endpoint ]);
    context.getSafe(KeysDerivedResourceIdentify.derivedResourceDescriptions).set(`${POD}.meta`, prefetched);
    const { discovered } = await actor(1).run(document(`${POD}Product1`));
    expect(discovered[0].resources).toBe(prefetched);
    expect(mediatorDereferenceRdf.mediate).not.toHaveBeenCalled();
  });
});
