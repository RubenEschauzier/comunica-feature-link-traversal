import { Bus } from '@comunica/core';
import { ActorQuerySourceIdentifyCache } from '../lib/ActorQuerySourceIdentifyCache';
import '@comunica/utils-jest';

describe('ActorQuerySourceIdentifyCache', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorQuerySourceIdentifyCache instance', () => {
    let actor: ActorQuerySourceIdentifyCache;

    beforeEach(() => {
      actor = new ActorQuerySourceIdentifyCache({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
