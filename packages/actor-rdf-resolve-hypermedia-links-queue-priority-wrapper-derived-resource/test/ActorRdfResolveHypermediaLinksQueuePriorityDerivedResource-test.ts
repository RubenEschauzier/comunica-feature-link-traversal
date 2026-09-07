import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueuePriorityDerivedResource } from '../lib/ActorRdfResolveHypermediaLinksQueuePriorityWrapperDerivedResource';
import '@comunica/utils-jest';

describe('ActorRdfResolveHypermediaLinksQueuePriorityDerivedResource', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueuePriorityDerivedResource instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueuePriorityDerivedResource;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueuePriorityDerivedResource({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
