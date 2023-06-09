import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueSolidLinkPrioritisation } from '../lib/ActorRdfResolveHypermediaLinksQueueSolidLinkPrioritisation';

describe('ActorRdfResolveHypermediaLinksQueueSolidLinkPrioritisation', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueSolidLinkPrioritisation instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueSolidLinkPrioritisation;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueSolidLinkPrioritisation({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
