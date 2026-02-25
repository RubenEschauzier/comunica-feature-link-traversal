import { Bus } from '@comunica/core';
import { ActorContextPreprocessSetDynamicLinksFilter } from '../lib/ActorContextPreprocessSetDynamicLinksFilter';
import '@comunica/utils-jest';

describe('ActorContextPreprocessSetDynamicLinksFilter', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorContextPreprocessSetDynamicLinksFilter instance', () => {
    let actor: ActorContextPreprocessSetDynamicLinksFilter;

    beforeEach(() => {
      actor = new ActorContextPreprocessSetDynamicLinksFilter({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
