import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritisation } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritisation';

describe('ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritisation', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritisation instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritisation;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritisation({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
