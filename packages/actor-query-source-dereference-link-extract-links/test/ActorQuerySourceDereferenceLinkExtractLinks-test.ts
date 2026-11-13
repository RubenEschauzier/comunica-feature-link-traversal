import { KeysInitQuery } from '@comunica/context-entries';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import { DataFactory } from 'rdf-data-factory';
import { ActorQuerySourceDereferenceLinkExtractLinks } from '../lib/ActorQuerySourceDereferenceLinkExtractLinks';
import { QuerySourceExtractLinks } from '../lib/QuerySourceExtractLinks';
import '@comunica/utils-jest';

const DF = new DataFactory();
const URL = 'http://ex.org/shop/sparql';

describe('ActorQuerySourceDereferenceLinkExtractLinks', () => {
  let actor: ActorQuerySourceDereferenceLinkExtractLinks;
  let mediatorQuerySourceDereferenceLink: any;
  let manager: any;
  let context: IActionContext;
  const inner = { referenceValue: URL };

  beforeEach(() => {
    mediatorQuerySourceDereferenceLink = {
      mediate: jest.fn(async() => ({ source: inner, metadata: { a: 1 }, dataset: 'd' })),
    };
    actor = new ActorQuerySourceDereferenceLinkExtractLinks({
      name: 'actor',
      bus: new Bus({ name: 'bus' }),
      mediatorQuerySourceDereferenceLink,
      mediatorExtractLinks: <any> { mediate: jest.fn() },
    });
    manager = {};
    context = new ActionContext({
      [KeysInitQuery.dataFactory.name]: DF,
      [KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager.name]: manager,
    });
  });

  describe('test', () => {
    it('accepts a seed given with a type during traversal', async() => {
      await expect(actor.test({ link: { url: URL, forceSourceType: 'sparql' }, context }))
        .resolves.toPassTestVoid();
    });

    it('rejects a link without a forced type', async() => {
      await expect(actor.test({ link: { url: URL }, context }))
        .resolves.toFailTest('actor only wraps the sources of seeds given with a type');
    });

    it('rejects a link outside traversal', async() => {
      await expect(actor.test({ link: { url: URL, forceSourceType: 'sparql' }, context: new ActionContext() }))
        .resolves.toFailTest('actor requires link traversal');
    });

    it('rejects a link it is wrapping already', async() => {
      await expect(actor.test({
        link: { url: URL, forceSourceType: 'sparql' },
        context: context.set((<any> ActorQuerySourceDereferenceLinkExtractLinks).keyWrapped, true),
      })).resolves.toFailTest('actor only wraps a source once');
    });
  });

  describe('run', () => {
    it('wraps the source the link dereferences to, and keeps the rest of the output', async() => {
      const output = await actor.run({ link: { url: URL, forceSourceType: 'sparql' }, context });
      expect(output.metadata).toEqual({ a: 1 });
      expect(output.dataset).toBe('d');
      expect(output.source).toBeInstanceOf(QuerySourceExtractLinks);
      expect((<QuerySourceExtractLinks> output.source).innerSource).toBe(inner);
      const { context: innerContext } = mediatorQuerySourceDereferenceLink.mediate.mock.calls[0][0];
      expect(innerContext.get((<any> ActorQuerySourceDereferenceLinkExtractLinks).keyWrapped)).toBe(true);
    });
  });
});
