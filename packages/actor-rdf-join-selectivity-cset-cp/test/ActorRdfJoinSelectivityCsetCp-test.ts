import { Bus } from '@comunica/core';
import { ActorRdfJoinSelectivityCsetCp } from '../lib/ActorRdfJoinSelectivityCsetCp';
import '@comunica/utils-jest';

describe('ActorRdfJoinSelectivityCsetCp', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfJoinSelectivityCsetCp instance', () => {
    let actor: ActorRdfJoinSelectivityCsetCp;

    beforeEach(() => {
      actor = new ActorRdfJoinSelectivityCsetCp({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toPassTestVoid(); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
