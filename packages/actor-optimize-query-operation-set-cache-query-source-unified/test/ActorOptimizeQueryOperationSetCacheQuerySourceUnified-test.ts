import { Bus } from '@comunica/core';
import { ActorOptimizeQueryOperationSetCacheQuerySourceUnified } from '../lib/ActorOptimizeQueryOperationSetCacheQuerySourceUnified';
import '@comunica/utils-jest';

describe('ActorOptimizeQueryOperationSetCacheQuerySourceUnified', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationSetCacheQuerySourceUnified instance', () => {
    let actor: ActorOptimizeQueryOperationSetCacheQuerySourceUnified;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationSetCacheQuerySourceUnified({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
