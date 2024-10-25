import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperIndegreePrioritisation } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperIndegreePrioritisation';

describe('ActorRdfResolveHypermediaLinksQueueWrapperIndegreePrioritisation', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperIndegreePrioritisation instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperIndegreePrioritisation;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperIndegreePrioritisation({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
