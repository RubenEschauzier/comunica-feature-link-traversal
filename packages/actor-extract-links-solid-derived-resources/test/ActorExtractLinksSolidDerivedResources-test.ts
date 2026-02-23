import { Bus } from '@comunica/core';
import { ActorExtractLinksSolidDerivedResources } from '../lib/ActorExtractLinksSolidDerivedResources';
import '@comunica/utils-jest';

describe('ActorExtractLinksSolidDerivedResources', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorExtractLinksSolidDerivedResources instance', () => {
    let actor: ActorExtractLinksSolidDerivedResources;

    beforeEach(() => {
      actor = new ActorExtractLinksSolidDerivedResources({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
