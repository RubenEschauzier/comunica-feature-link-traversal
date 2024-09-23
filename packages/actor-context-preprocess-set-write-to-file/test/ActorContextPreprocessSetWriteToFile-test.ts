import { Bus } from '@comunica/core';
import { ActorContextPreprocessSetWriteToFile } from '../lib/ActorContextPreprocessSetWriteToFile';

describe('ActorContextPreprocessSetWriteToFile', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorContextPreprocessSetWriteToFile instance', () => {
    let actor: ActorContextPreprocessSetWriteToFile;

    beforeEach(() => {
      actor = new ActorContextPreprocessSetWriteToFile({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
