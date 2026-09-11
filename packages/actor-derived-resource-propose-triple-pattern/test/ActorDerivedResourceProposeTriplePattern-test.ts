import { Bus } from '@comunica/core';
import { ActorDerivedResourceProposeTriplePattern } from '../lib/ActorDerivedResourceProposeTriplePattern';
import '@comunica/utils-jest';

describe('ActorDerivedResourceProposeTriplePattern', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourceProposeTriplePattern instance', () => {
    let actor: ActorDerivedResourceProposeTriplePattern;

    beforeEach(() => {
      actor = new ActorDerivedResourceProposeTriplePattern({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
