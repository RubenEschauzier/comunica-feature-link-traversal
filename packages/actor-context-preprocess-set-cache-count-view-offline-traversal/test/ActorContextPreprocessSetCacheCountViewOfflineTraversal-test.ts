import { Bus } from '@comunica/core';
import { ActorContextPreprocessSetCacheCountViewOfflineTraversal } from '../lib/ActorContextPreprocessSetCacheCountViewOfflineTraversal';
import '@comunica/utils-jest';

describe('ActorContextPreprocessSetCacheCountViewOfflineTraversal', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorContextPreprocessSetCacheCountViewOfflineTraversal instance', () => {
    let actor: ActorContextPreprocessSetCacheCountViewOfflineTraversal;

    beforeEach(() => {
      actor = new ActorContextPreprocessSetCacheCountViewOfflineTraversal({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
