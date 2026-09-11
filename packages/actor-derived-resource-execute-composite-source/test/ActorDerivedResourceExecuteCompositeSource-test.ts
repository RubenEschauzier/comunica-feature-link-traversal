import { Bus } from '@comunica/core';
import { ActorDerivedResourceExecuteCompositeSource } from '../lib/ActorDerivedResourceExecuteCompositeSource';
import '@comunica/utils-jest';

describe('ActorDerivedResourceExecuteCompositeSource', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourceExecuteCompositeSource instance', () => {
    let actor: ActorDerivedResourceExecuteCompositeSource;

    beforeEach(() => {
      actor = new ActorDerivedResourceExecuteCompositeSource({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
