import { Bus } from '@comunica/core';
import { ActorContextPreprocessSetSourceTracking } from '../lib/ActorContextPreprocessSetSourceTracking';
import '@comunica/utils-jest';

describe('ActorContextPreprocessSetSourceTracking', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorContextPreprocessSetSourceTracking instance', () => {
    let actor: ActorContextPreprocessSetSourceTracking;

    beforeEach(() => {
      actor = new ActorContextPreprocessSetSourceTracking({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
