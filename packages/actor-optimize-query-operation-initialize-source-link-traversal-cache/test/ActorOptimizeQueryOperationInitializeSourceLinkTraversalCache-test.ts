import { Bus } from '@comunica/core';
import { ActorOptimizeQueryOperationInitializeSourceLinkTraversalCache } from '../lib/ActorOptimizeQueryOperationInitializeSourceLinkTraversalCache';
import '@comunica/utils-jest';

describe('ActorOptimizeQueryOperationInitializeSourceLinkTraversalCache', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationInitializeSourceLinkTraversalCache instance', () => {
    let actor: ActorOptimizeQueryOperationInitializeSourceLinkTraversalCache;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationInitializeSourceLinkTraversalCache({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
