import { KeysInitQuery, KeysQueryOperation } from '@comunica/context-entries';
import { KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { ActionContext } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import { ActorExtractLinksSolidDerivedResources } from '../lib/ActorExtractLinksSolidDerivedResources';

const POD = 'http://fedshop-proxy:3000/producer2/';
const KNOWN = 'http://fedshop-proxy:34300/taxonomy/sparql';
const DISCOVERED = 'http://fedshop-proxy:34343/producer2/sparql';

describe('ActorExtractLinksSolidDerivedResources claiming resources', () => {
  const actor: any = Object.create(ActorExtractLinksSolidDerivedResources.prototype);
  let exact: Set<string>;
  let globs: Set<string>;
  let context: IActionContext;
  const endpoint = (template: string, selectors: string[] = []): any =>
    ({ baseUrl: POD, template, selectors, filter: '' });

  beforeEach(() => {
    exact = new Set();
    globs = new Set();
    context = new ActionContext({
      [KeysRdfResolveHypermediaLinks.dynamicFilter.name]: {
        addExact: (url: string) => exact.add(url),
        addGlob: (selector: string) => globs.add(selector),
      },
      [KeysInitQuery.querySourcesUnidentified.name]: [
        'http://fedshop-proxy:3000/taxonomy/countries',
        { type: 'sparql', value: KNOWN },
      ],
    });
  });

  it('claims a discovered endpoint once per query, and keeps it off the crawl', () => {
    expect(actor.claimResources(context, [ endpoint(DISCOVERED) ])).toEqual([ endpoint(DISCOVERED) ]);
    expect(actor.claimResources(context, [ endpoint(DISCOVERED) ])).toEqual([]);
    expect(exact).toEqual(new Set([ DISCOVERED ]));
    expect(globs).toEqual(new Set());
  });

  it('does not take up an endpoint that is already a source of the query', () => {
    expect(actor.claimResources(context, [ endpoint(KNOWN), endpoint(DISCOVERED) ]))
      .toEqual([ endpoint(DISCOVERED) ]);
    expect(exact).toEqual(new Set([ KNOWN, DISCOVERED ]));
  });

  it('takes the documents covered by an endpoint that is already a source of the query off the crawl', () => {
    const covered = endpoint(KNOWN, [ `${POD}**`, `${POD}index` ]);
    expect(actor.claimResources(context, [ covered ])).toEqual([]);
    expect(globs).toEqual(new Set([ `${POD}**` ]));
    expect(exact).toEqual(new Set([ KNOWN, `${POD}index` ]));
  });

  it('does not take up an endpoint that is a source of the query once the sources are identified', () => {
    const identified = context
      .delete(KeysInitQuery.querySourcesUnidentified)
      .set(KeysQueryOperation.querySources, [{ source: <any> { referenceValue: KNOWN }}]);
    expect(actor.claimResources(identified, [ endpoint(KNOWN), endpoint(DISCOVERED) ]))
      .toEqual([ endpoint(DISCOVERED) ]);
  });

  it('claims an endpoint whose url is only given as an untyped source, which is traversed as a document', () => {
    const untyped = 'http://fedshop-proxy:3000/taxonomy/countries';
    expect(actor.claimResources(context, [ endpoint(untyped) ])).toEqual([ endpoint(untyped) ]);
  });
});
