import { Bus } from '@comunica/core';
import { ActorDerivedResourceSelectTriplePattern } from '../lib/ActorDerivedResourceSelectTriplePattern';
import '@comunica/utils-jest';

describe('ActorDerivedResourceSelectTriplePattern', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourceSelectTriplePattern instance', () => {
    let actor: ActorDerivedResourceSelectTriplePattern;

    beforeEach(() => {
      actor = new ActorDerivedResourceSelectTriplePattern({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
