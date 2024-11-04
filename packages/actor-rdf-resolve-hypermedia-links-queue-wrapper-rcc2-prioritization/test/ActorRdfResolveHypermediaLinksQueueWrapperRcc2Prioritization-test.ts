import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritization } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritization';

describe('ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritization', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritization instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritization;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperRcc2Prioritization({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
