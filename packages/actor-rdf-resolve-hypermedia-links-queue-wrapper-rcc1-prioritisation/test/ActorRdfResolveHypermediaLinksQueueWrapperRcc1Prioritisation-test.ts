import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritisation } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritisation';

describe('ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritisation', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritisation instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritisation;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritisation({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
