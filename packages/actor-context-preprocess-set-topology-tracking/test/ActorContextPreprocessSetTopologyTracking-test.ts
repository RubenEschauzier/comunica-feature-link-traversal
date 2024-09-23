import { Bus } from '@comunica/core';
import { ActorContextPreprocessSetTopologyTracking } from '../lib/ActorContextPreprocessSetTopologyTracking';

describe('ActorContextPreprocessSetTopologyTracking', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorContextPreprocessSetTopologyTracking instance', () => {
    let actor: ActorContextPreprocessSetTopologyTracking;

    beforeEach(() => {
      actor = new ActorContextPreprocessSetTopologyTracking({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
