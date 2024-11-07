import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperIsdcrPrioritization } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperIsdcrPrioritization';

describe('ActorRdfResolveHypermediaLinksQueueWrapperIsdcrPrioritization', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperIsdcrPrioritization instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperIsdcrPrioritization;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperIsdcrPrioritization({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
