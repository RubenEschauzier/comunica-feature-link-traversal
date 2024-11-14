import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperPagerankPrioritisation } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperPagerankPrioritisation';

describe('ActorRdfResolveHypermediaLinksQueueWrapperPagerankPrioritisation', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperPagerankPrioritisation instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperPagerankPrioritisation;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperPagerankPrioritisation({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});