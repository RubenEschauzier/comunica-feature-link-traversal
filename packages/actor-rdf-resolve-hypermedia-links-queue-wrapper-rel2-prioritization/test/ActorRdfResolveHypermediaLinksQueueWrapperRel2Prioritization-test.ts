import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritization } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritization';

describe('ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritization', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritization instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritization;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperRel2Prioritization({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
