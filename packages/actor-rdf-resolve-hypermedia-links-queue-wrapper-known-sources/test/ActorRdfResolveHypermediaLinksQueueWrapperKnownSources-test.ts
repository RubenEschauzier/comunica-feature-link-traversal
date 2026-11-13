import { LinkQueueFifo } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-fifo';
import type { MediatorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { KeysCore, KeysInitQuery, KeysQuerySourceIdentify } from '@comunica/context-entries';
import { KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import {
  ActorRdfResolveHypermediaLinksQueueWrapperKnownSources,
} from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperKnownSources';
import { LinkQueueWrapperKnownSources } from '../lib/LinkQueueWrapperKnownSources';
import '@comunica/utils-jest';

const SHOP = 'http://ex.org:8080/shop/sparql';
const REVIEWS = 'http://ex.org:8080/reviews/sparql';
const NAMESPACES = {
  [KeysRdfResolveHypermediaLinks.sourceNamespaces.name]: { [SHOP]: [ 'http://ex.org/shop/' ]},
};

function contextWith(sources: any[], entries: Record<string, any> = {}): IActionContext {
  return new ActionContext({ [KeysInitQuery.querySourcesUnidentified.name]: sources, ...entries });
}

describe('ActorRdfResolveHypermediaLinksQueueWrapperKnownSources', () => {
  let mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
  let actor: ActorRdfResolveHypermediaLinksQueueWrapperKnownSources;

  beforeEach(() => {
    mediatorRdfResolveHypermediaLinksQueue = <any> {
      mediate: jest.fn(async() => ({ linkQueue: new LinkQueueFifo() })),
    };
    actor = new ActorRdfResolveHypermediaLinksQueueWrapperKnownSources({
      bus: new Bus({ name: 'bus' }),
      name: 'actor',
      mediatorRdfResolveHypermediaLinksQueue,
    });
  });

  describe('knownSourceUrls', () => {
    it('keeps the sources given with a type, whether traversed or not', () => {
      const store = { match: jest.fn() };
      expect(ActorRdfResolveHypermediaLinksQueueWrapperKnownSources.knownSourceUrls(contextWith([
        'http://ex.org/seed',
        { type: 'sparql', value: SHOP },
        { value: 'http://ex.org/untyped' },
        { type: 'rdfjs', value: store },
        store,
        { type: 'sparql', value: REVIEWS, context: { [KeysQuerySourceIdentify.traverse.name]: true }},
      ], { [KeysQuerySourceIdentify.traverse.name]: true }))).toEqual([ SHOP, REVIEWS ]);
    });

    it('keeps nothing without sources', () => {
      expect(ActorRdfResolveHypermediaLinksQueueWrapperKnownSources.knownSourceUrls(new ActionContext()))
        .toEqual([]);
    });
  });

  describe('knownNamespaces', () => {
    it('keeps the namespaces of the given sources only', () => {
      const context = contextWith([], {
        [KeysRdfResolveHypermediaLinks.sourceNamespaces.name]: {
          [SHOP]: [ 'http://ex.org/shop/' ],
          [REVIEWS]: [ 'http://ex.org/reviews/' ],
        },
      });
      expect(ActorRdfResolveHypermediaLinksQueueWrapperKnownSources.knownNamespaces(context, [ SHOP, 'http://x' ]))
        .toEqual([ 'http://ex.org/shop/' ]);
    });

    it('keeps nothing without declared namespaces', () => {
      expect(ActorRdfResolveHypermediaLinksQueueWrapperKnownSources.knownNamespaces(new ActionContext(), [ SHOP ]))
        .toEqual([]);
    });
  });

  describe('test', () => {
    it('accepts a context with a known source with namespaces', async() => {
      await expect(actor.test({ context: contextWith([{ type: 'sparql', value: SHOP }], NAMESPACES) }))
        .resolves.toPassTestVoid();
    });

    it('rejects a known source without namespaces', async() => {
      await expect(actor.test({ context: contextWith([{ type: 'sparql', value: SHOP }]) }))
        .resolves.toFailTest('No namespaces of known query sources to keep traversal out of');
    });

    it('rejects when wrapped already', async() => {
      await expect(actor.test({
        context: contextWith([{ type: 'sparql', value: SHOP }], {
          ...NAMESPACES,
          [(<any> ActorRdfResolveHypermediaLinksQueueWrapperKnownSources).keyWrapped.name]: true,
        }),
      })).resolves.toFailTest('Unable to wrap link queues multiple times');
    });
  });

  describe('run', () => {
    it('wraps the queue to refuse the namespaces of the known sources', async() => {
      const { linkQueue } = await actor.run({
        context: contextWith([ 'http://ex.org/seed', { type: 'sparql', value: SHOP }], {
          [KeysRdfResolveHypermediaLinks.sourceNamespaces.name]: { [SHOP]: [ 'http://ex.org/shop/' ]},
        }),
      });
      expect(linkQueue).toBeInstanceOf(LinkQueueWrapperKnownSources);
      expect(linkQueue.push({ url: SHOP })).toBe(true);
      expect(linkQueue.push({ url: 'http://ex.org/shop/product1' })).toBe(false);
      expect(linkQueue.push({ url: 'http://ex.org/seed' })).toBe(true);
      expect((<any> mediatorRdfResolveHypermediaLinksQueue.mediate).mock.calls[0][0].context
        .get((<any> ActorRdfResolveHypermediaLinksQueueWrapperKnownSources).keyWrapped)).toBe(true);
    });

    it('logs the known sources and namespaces', async() => {
      const logger = { debug: jest.fn() };
      await actor.run({
        context: contextWith([{ type: 'sparql', value: SHOP }], {
          [KeysCore.log.name]: logger,
          [KeysRdfResolveHypermediaLinks.sourceNamespaces.name]: { [SHOP]: [ 'http://ex.org/shop/' ]},
        }),
      });
      expect(logger.debug).toHaveBeenCalledWith('Keeping traversal out of the namespaces of known sources', {
        actor: 'actor',
        sourceUrls: [ SHOP ],
        namespaces: [ 'http://ex.org/shop/' ],
      });
    });
  });
});
