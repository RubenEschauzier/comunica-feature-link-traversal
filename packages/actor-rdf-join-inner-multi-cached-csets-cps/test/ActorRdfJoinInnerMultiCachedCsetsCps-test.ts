import { Bus } from '@comunica/core';
import { ActorRdfJoinInnerMultiCachedCsetsCps } from '../lib/ActorRdfJoinInnerMultiCachedCsetsCps';
import '@comunica/utils-jest';

describe('ActorRdfJoinInnerMultiCachedCsetsCps', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfJoinInnerMultiCachedCsetsCps instance', () => {
    let actor: ActorRdfJoinInnerMultiCachedCsetsCps;

    beforeEach(() => {
      actor = new ActorRdfJoinInnerMultiCachedCsetsCps({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
