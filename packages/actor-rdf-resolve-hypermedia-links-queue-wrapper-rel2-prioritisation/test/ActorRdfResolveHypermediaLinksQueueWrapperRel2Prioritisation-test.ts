import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritisation } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritisation';

describe('ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritisation', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritisation instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritisation;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritisation({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
