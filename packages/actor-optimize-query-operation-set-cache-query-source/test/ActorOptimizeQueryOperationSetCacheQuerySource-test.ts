import { Bus } from '@comunica/core';
import { ActorOptimizeQueryOperationSetCacheQuerySource } from '../lib/ActorOptimizeQueryOperationSetCacheQuerySource';
import '@comunica/utils-jest';

describe('ActorOptimizeQueryOperationSetCacheQuerySource', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationSetCacheQuerySource instance', () => {
    let actor: ActorOptimizeQueryOperationSetCacheQuerySource;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationSetCacheQuerySource({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
