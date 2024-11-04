import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritization } from '../lib/ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritization';

describe('ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritization', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritization instance', () => {
    let actor: ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritization;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaLinksQueueWrapperRcc1Prioritization({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
