import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperIsPrioritization } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperIsPrioritization';

describe('ActorRdfResolveHypermediaLinksQueueWrapperIsPrioritization', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperIsPrioritization instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperIsPrioritization;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperIsPrioritization({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
