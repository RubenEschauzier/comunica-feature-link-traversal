import { Bus } from '@comunica/core';
import { ActorDerivedResourceIdentifyTriplePatternQuery } from '../lib/ActorDerivedResourceIdentifyTriplePatternQuery';
import '@comunica/utils-jest';

describe('ActorDerivedResourceIdentifyTriplePatternQuery', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourceIdentifyTriplePatternQuery instance', () => {
    let actor: ActorDerivedResourceIdentifyTriplePatternQuery;

    beforeEach(() => {
      actor = new ActorDerivedResourceIdentifyTriplePatternQuery({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
