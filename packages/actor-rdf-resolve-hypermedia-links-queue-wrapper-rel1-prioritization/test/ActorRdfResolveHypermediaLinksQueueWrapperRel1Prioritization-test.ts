import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritization } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritization';

describe('ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritization', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritization instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritization;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperRel1Prioritization({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
