import { Bus } from '@comunica/core';
import { ActorContextPreprocessSetCacheCountView } from '../lib/ActorContextPreprocessSetCacheCountView';
import '@comunica/utils-jest';

describe('ActorContextPreprocessSetCacheCountView', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorContextPreprocessSetCacheCountView instance', () => {
    let actor: ActorContextPreprocessSetCacheCountView;

    beforeEach(() => {
      actor = new ActorContextPreprocessSetCacheCountView({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
