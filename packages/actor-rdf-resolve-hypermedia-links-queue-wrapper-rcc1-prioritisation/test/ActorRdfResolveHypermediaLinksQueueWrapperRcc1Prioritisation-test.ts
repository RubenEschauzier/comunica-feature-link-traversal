import type {
  IActionRdfResolveHypermediaLinksQueue,
  IActorRdfResolveHypermediaLinksQueueOutput,
} from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import { KEY_CONTEXT_WRAPPED, LinkQueueRcc1Prioritisation } from '..';
import {
  ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritisation,
} from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritisation';
import { IActionConstructTraversedTopology, IActorConstructTraversedTopologyOutput} from '@comunica/bus-construct-traversed-topology';
describe('ActorRdfResolveHypermediaLinksQueueWrapperLimitDepth', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperLimitDepth instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritisation;
    let mediatorRdfResolveHypermediaLinksQueue: Mediator<
    Actor<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>,
    IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput>;
    let mediatorConstructTraversedTopology: Mediator<
    Actor<IActionConstructTraversedTopology, IActorTest, IActorConstructTraversedTopologyOutput>,
    IActionConstructTraversedTopology, IActorTest, IActorConstructTraversedTopologyOutput
    >;

    beforeEach(() => {
      mediatorRdfResolveHypermediaLinksQueue = <any> {
        mediate: jest.fn(() => ({ linkQueue: 'inner' })),
      };
      mediatorConstructTraversedTopology = <any> {
        mediate: jest.fn(() => ({}))
      }
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritisation(
        { name: 'actor', bus, mediatorRdfResolveHypermediaLinksQueue, mediatorConstructTraversedTopology },
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
      })).rejects.toThrowError('Unable to wrap link queues multiple times');
    });

    it('should run', async() => {
      expect(await actor.run({ firstUrl: 'first', context: new ActionContext() })).toMatchObject({
        linkQueue: new LinkQueueRcc1Prioritisation(<any> 'inner', 10),
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
