import { Bus } from '@comunica/core';
import { ActorDerivedResourceProposeStars } from '../lib/ActorDerivedResourceProposeStars';
import '@comunica/utils-jest';

describe('ActorDerivedResourceProposeStars', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourceProposeStars instance', () => {
    let actor: ActorDerivedResourceProposeStars;

    beforeEach(() => {
      actor = new ActorDerivedResourceProposeStars({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
