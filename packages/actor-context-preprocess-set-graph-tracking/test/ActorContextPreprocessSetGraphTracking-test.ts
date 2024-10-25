import { Bus } from '@comunica/core';
import { ActorContextPreprocessSetGraphTracking } from '../lib/ActorContextPreprocessSetGraphTracking';

describe('ActorContextPreprocessSetGraphTracking', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorContextPreprocessSetGraphTracking instance', () => {
    let actor: ActorContextPreprocessSetGraphTracking;

    beforeEach(() => {
      actor = new ActorContextPreprocessSetGraphTracking({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
