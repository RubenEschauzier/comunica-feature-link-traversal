import { Bus } from '@comunica/core';
import { ActorOptimizeQueryOperationSetCacheCsetOfflineTraversal } from '../lib/ActorOptimizeQueryOperationSetCacheCsetOfflineTraversal';
import '@comunica/utils-jest';

describe('ActorOptimizeQueryOperationSetCacheCsetOfflineTraversal', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationSetCacheCsetOfflineTraversal instance', () => {
    let actor: ActorOptimizeQueryOperationSetCacheCsetOfflineTraversal;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationSetCacheCsetOfflineTraversal({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
