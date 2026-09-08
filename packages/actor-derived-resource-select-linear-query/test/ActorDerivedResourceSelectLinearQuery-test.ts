import { Bus } from '@comunica/core';
import { ActorDerivedResourceSelectLinearQuery } from '../lib/ActorDerivedResourceSelectLinearQuery';
import '@comunica/utils-jest';

describe('ActorDerivedResourceSelectLinearQuery', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourceSelectLinearQuery instance', () => {
    let actor: ActorDerivedResourceSelectLinearQuery;

    beforeEach(() => {
      actor = new ActorDerivedResourceSelectLinearQuery({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
