import type { MediatorHttp } from '@comunica/bus-http';
import { ActionContext, Bus } from '@comunica/core';
import { ActorHttpDelay } from '../lib/ActorHttpDelay';
import '@comunica/utils-jest';

describe('ActorHttpDelay', () => {
  let bus: any;
  let actor: ActorHttpDelay;
  let mediatorHttp: MediatorHttp;

  const average = 40;
  const delta = 0;
  const url = 'http://localhost:3000/some/url';

  beforeEach(() => {
    jest.resetAllMocks();
    jest.restoreAllMocks();
    bus = new Bus({ name: 'bus' });
    mediatorHttp = <any>{
      mediate: jest.fn().mockRejectedValue(new Error('mediatorHttp.mediate')),
    };
    actor = new ActorHttpDelay({
      bus,
      mediatorHttp,
      name: 'actor',
      average,
      delta,
    });
    jest.spyOn((<any>actor), 'logDebug').mockImplementation((...args) => (<() => unknown>args[2])());
  });

  describe('test', () => {
    it('should wrap operation', async() => {
      await expect(actor.test({
        context: new ActionContext({}),
        input: url,
      })).resolves.toPassTest({ time: 0 });
    });

    it('should wrap operation only once', async() => {
      await expect(actor.test({
        context: new ActionContext({ [(<any>ActorHttpDelay).keyWrapped.name]: true }),
        input: url,
      })).resolves.toFailTest(`${actor.name} can only wrap a request once`);
    });

    it('should refuse to run with no latency range', async() => {
      (<any>actor).minimum = 0;
      (<any>actor).maximum = 0;
      await expect(actor.test({
        context: new ActionContext({}),
        input: url,
      })).resolves.toFailTest(`${actor.name} has no latency range available`);
    });
  });

  describe('run', () => {
    it('should delay requests', async() => {
      const response = { ok: true };
      const duration = 100;
      jest.spyOn(Date, 'now').mockReturnValueOnce(0).mockReturnValueOnce(duration);
      jest.spyOn(mediatorHttp, 'mediate').mockResolvedValue(<any>response);
      jest.spyOn(globalThis, 'setTimeout').mockImplementation(callback => <any>callback());
      const action = { context: new ActionContext({}), input: url };
      await expect(actor.run(action)).resolves.toEqual(response);
      expect(globalThis.setTimeout).toHaveBeenCalledTimes(1);
      expect(globalThis.setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), average);
    });

    it('should forward mediator errors', async() => {
      const errorMessage = 'HTTP error';
      jest.spyOn(Date, 'now').mockReturnValueOnce(0);
      jest.spyOn(Date, 'now').mockReturnValueOnce(100);
      jest.spyOn(mediatorHttp, 'mediate').mockRejectedValue(new Error(errorMessage));
      jest.spyOn(globalThis, 'setTimeout').mockImplementation(callback => <any>callback());
      const action = { context: new ActionContext({}), input: url };
      await expect(actor.run(action)).rejects.toThrow(errorMessage);
      expect(globalThis.setTimeout).toHaveBeenCalledTimes(1);
      expect(globalThis.setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), average);
    });
  });
});
