import { Bus } from '@comunica/core';
import { ActorContextPreprocessSetLinksFilter } from '../lib/ActorContextPreprocessSetLinksFilter';
import '@comunica/utils-jest';

describe('ActorContextPreprocessSetLinksFilter', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorContextPreprocessSetLinksFilter instance', () => {
    let actor: ActorContextPreprocessSetLinksFilter;

    beforeEach(() => {
      actor = new ActorContextPreprocessSetLinksFilter({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
