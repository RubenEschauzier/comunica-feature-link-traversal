import { Bus } from '@comunica/core';
import { ActorQuerySourceDereferenceLinkHypermediaWrapCacheQuerySource } from '../lib/ActorQuerySourceDereferenceLinkHypermediaWrapCacheQuerySource';
import '@comunica/utils-jest';

describe('ActorQuerySourceDereferenceLinkHypermediaWrapCacheQuerySource', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorQuerySourceDereferenceLinkHypermediaWrapCacheQuerySource instance', () => {
    let actor: ActorQuerySourceDereferenceLinkHypermediaWrapCacheQuerySource;

    beforeEach(() => {
      actor = new ActorQuerySourceDereferenceLinkHypermediaWrapCacheQuerySource({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
