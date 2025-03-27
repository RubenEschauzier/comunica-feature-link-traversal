import { Bus } from '@comunica/core';
import { ActorExtractSourcesCache } from '../lib/ActorExtractSourcesCache';

describe('ActorExtractSourcesCache', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorExtractSourcesCache instance', () => {
    let actor: ActorExtractSourcesCache;

    beforeEach(() => {
      actor = new ActorExtractSourcesCache({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
