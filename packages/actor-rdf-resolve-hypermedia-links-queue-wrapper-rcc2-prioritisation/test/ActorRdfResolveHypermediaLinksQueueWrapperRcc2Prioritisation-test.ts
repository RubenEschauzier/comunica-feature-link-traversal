import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritisation } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritisation';

describe('ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritisation', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritisation instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritisation;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritisation({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
