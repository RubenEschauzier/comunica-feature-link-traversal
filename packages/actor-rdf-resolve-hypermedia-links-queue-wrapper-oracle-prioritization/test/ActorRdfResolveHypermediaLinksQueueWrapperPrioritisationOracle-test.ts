import type {
  IActionRdfResolveHypermediaLinksQueue,
  IActorRdfResolveHypermediaLinksQueueOutput,
} from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import { KEY_CONTEXT_WRAPPED, LinkQueuePriorityOracle } from '../lib';
import {
  ActorRdfResolveHypermediaLinksQueueWrapperPrioritisationOracle,
} from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperPrioritisationOracle';

describe('ActorRdfResolveHypermediaLinksQueueWrapperPrioritisationOracle', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperPrioritisationOracle instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperPrioritisationOracle;
    let mediatorRdfResolveHypermediaLinksQueue: Mediator<
    Actor<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>,
    IActionRdfResolveHypermediaLinksQueue,
IActorTest,
IActorRdfResolveHypermediaLinksQueueOutput
>;

    beforeEach(() => {
      mediatorRdfResolveHypermediaLinksQueue = <any> {
        mediate: jest.fn(() => ({ linkQueue: 'inner' })),
      };
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperPrioritisationOracle(
        { name: 'actor', bus, mediatorRdfResolveHypermediaLinksQueue },
      );
    });

    it('should test', () => {
      return expect(actor.test({ firstUrl: 'first', context: new ActionContext() })).resolves.toBeTruthy();
    });

    it('should not test when called recursively', () => {
      return expect(actor.test({
        firstUrl: 'first',
        context: new ActionContext({
          [KEY_CONTEXT_WRAPPED.name]: true,
        }),
      })).rejects.toThrow('Unable to wrap link queues multiple times');
    });

    it('should run', async() => {
      await expect(actor.run({ firstUrl: 'first', context: new ActionContext() })).resolves.toMatchObject({
        linkQueue: new LinkQueuePriorityOracle(<any> 'inner', { url: 5 }),
      });
      expect(mediatorRdfResolveHypermediaLinksQueue.mediate).toHaveBeenCalledWith({
        firstUrl: 'first',
        context: new ActionContext({
          [KEY_CONTEXT_WRAPPED.name]: true,
        }),
      });
    });
  });
});
