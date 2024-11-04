import { Bus } from '@comunica/core';
import { ActorContextPreprocessSetGraphTrackingRcc } from '../lib/ActorContextPreprocessSetGraphTrackingRcc';

describe('ActorContextPreprocessSetGraphTrackingRcc', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorContextPreprocessSetGraphTrackingRcc instance', () => {
    let actor: ActorContextPreprocessSetGraphTrackingRcc;

    beforeEach(() => {
      actor = new ActorContextPreprocessSetGraphTrackingRcc({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
