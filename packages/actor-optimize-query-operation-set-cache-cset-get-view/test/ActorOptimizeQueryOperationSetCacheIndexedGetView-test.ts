import { Bus } from '@comunica/core';
import { ActorOptimizeQueryOperationSetCacheIndexedGetView } from '../lib/ActorOptimizeQueryOperationSetCacheIndexedGetView';
import '@comunica/utils-jest';

describe('ActorOptimizeQueryOperationSetCacheIndexedGetView', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationSetCacheIndexedGetView instance', () => {
    let actor: ActorOptimizeQueryOperationSetCacheIndexedGetView;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationSetCacheIndexedGetView({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
