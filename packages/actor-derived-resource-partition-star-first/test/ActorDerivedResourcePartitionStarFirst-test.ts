import { Bus } from '@comunica/core';
import { ActorDerivedResourcePartitionStarFirst } from '../lib/ActorDerivedResourcePartitionStarFirst';
import '@comunica/utils-jest';

describe('ActorDerivedResourcePartitionStarFirst', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourcePartitionStarFirst instance', () => {
    let actor: ActorDerivedResourcePartitionStarFirst;

    beforeEach(() => {
      actor = new ActorDerivedResourcePartitionStarFirst({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
