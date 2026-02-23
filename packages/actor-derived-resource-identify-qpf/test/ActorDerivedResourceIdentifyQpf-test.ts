import { Bus } from '@comunica/core';
import { ActorDerivedResourceIdentifyQpf } from '../lib/ActorDerivedResourceIdentifyQpf';
import '@comunica/utils-jest';

describe('ActorDerivedResourceIdentifyQpf', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorDerivedResourceIdentifyQpf instance', () => {
    let actor: ActorDerivedResourceIdentifyQpf;

    beforeEach(() => {
      actor = new ActorDerivedResourceIdentifyQpf({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
