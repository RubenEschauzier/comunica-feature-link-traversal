import { Bus } from '@comunica/core';
import { ActorDerivedResourceSelectStarQuery } from '../lib/ActorDerivedResourceSelectStarQuery';
import '@comunica/utils-jest';

describe('ActorDerivedResourceSelectStarQuery', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourceSelectStarQuery instance', () => {
    let actor: ActorDerivedResourceSelectStarQuery;

    beforeEach(() => {
      actor = new ActorDerivedResourceSelectStarQuery({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
