import { Bus } from '@comunica/core';
import { ActorDerivedResourceProposeChain } from '../lib/ActorDerivedResourceProposeChain';
import '@comunica/utils-jest';

describe('ActorDerivedResourceProposeChain', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourceProposeChain instance', () => {
    let actor: ActorDerivedResourceProposeChain;

    beforeEach(() => {
      actor = new ActorDerivedResourceProposeChain({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
