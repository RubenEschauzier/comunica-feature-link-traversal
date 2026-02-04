import { Bus } from '@comunica/core';
import { ActorOptimizeQueryOperationSetCacheQuerySourceAggregated } from '../lib/ActorOptimizeQueryOperationSetCacheQuerySourceAggregated';
import '@comunica/utils-jest';

describe('ActorOptimizeQueryOperationSetCacheQuerySourceAggregated', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationSetCacheQuerySourceAggregated instance', () => {
    let actor: ActorOptimizeQueryOperationSetCacheQuerySourceAggregated;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationSetCacheQuerySourceAggregated({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
