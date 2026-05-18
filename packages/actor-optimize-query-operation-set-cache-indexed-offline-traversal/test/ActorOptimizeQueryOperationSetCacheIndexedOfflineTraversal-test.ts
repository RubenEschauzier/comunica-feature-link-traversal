import { Bus } from '@comunica/core';
import { ActorOptimizeQueryOperationSetCacheIndexedOfflineTraversal } from '../lib/ActorOptimizeQueryOperationSetCacheIndexedOfflineTraversal';
import '@comunica/utils-jest';

describe('ActorOptimizeQueryOperationSetCacheIndexedOfflineTraversal', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationSetCacheIndexedOfflineTraversal instance', () => {
    let actor: ActorOptimizeQueryOperationSetCacheIndexedOfflineTraversal;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationSetCacheIndexedOfflineTraversal({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
