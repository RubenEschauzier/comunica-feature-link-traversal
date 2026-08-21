import { Bus } from '@comunica/core';
import { ActorDerivedResourceIdentifyStarQuery } from '../lib/ActorDerivedResourceIdentifyStarQuery';
import '@comunica/utils-jest';

describe('ActorDerivedResourceIdentifyStarQuery', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourceIdentifyStarQuery instance', () => {
    let actor: ActorDerivedResourceIdentifyStarQuery;

    beforeEach(() => {
      actor = new ActorDerivedResourceIdentifyStarQuery({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
