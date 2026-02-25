import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperFilterDynamic } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperFilterDynamic';
import '@comunica/utils-jest';

describe('ActorRdfResolveHypermediaLinksQueueWrapperFilterDynamic', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperFilterDynamic instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperFilterDynamic;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperFilterDynamic({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
