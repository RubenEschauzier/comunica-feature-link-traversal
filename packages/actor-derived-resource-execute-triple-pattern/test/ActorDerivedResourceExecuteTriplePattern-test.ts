import { Bus } from '@comunica/core';
import { ActorDerivedResourceExecuteTriplePattern } from '../lib/ActorDerivedResourceExecuteTriplePattern';
import '@comunica/utils-jest';

describe('ActorDerivedResourceExecuteTriplePattern', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourceExecuteTriplePattern instance', () => {
    let actor: ActorDerivedResourceExecuteTriplePattern;

    beforeEach(() => {
      actor = new ActorDerivedResourceExecuteTriplePattern({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
