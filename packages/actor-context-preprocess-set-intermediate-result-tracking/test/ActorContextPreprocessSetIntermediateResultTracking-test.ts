import { Bus } from '@comunica/core';
import { ActorContextPreprocessSetIntermediateResultTracking } from '../lib/ActorContextPreprocessSetIntermediateResultTracking';

describe('ActorContextPreprocessSetIntermediateResultTracking', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorContextPreprocessSetIntermediateResultTracking instance', () => {
    let actor: ActorContextPreprocessSetIntermediateResultTracking;

    beforeEach(() => {
      actor = new ActorContextPreprocessSetIntermediateResultTracking({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
