import { Bus } from '@comunica/core';
import { ActorOptimizeQueryOperationSetCacheIndexed } from '../lib/ActorOptimizeQueryOperationSetCacheIndexed';
import '@comunica/utils-jest';

describe('ActorOptimizeQueryOperationSetCacheIndexed', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationSetCacheIndexed instance', () => {
    let actor: ActorOptimizeQueryOperationSetCacheIndexed;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationSetCacheIndexed({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
