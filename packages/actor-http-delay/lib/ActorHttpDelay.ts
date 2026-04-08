import type { IActionHttp, IActorHttpOutput, IActorHttpArgs, MediatorHttp } from '@comunica/bus-http';
import { ActorHttp } from '@comunica/bus-http';
import type { TestResult } from '@comunica/core';
import { ActionContextKey, failTest, passTest } from '@comunica/core';
import type { IMediatorTypeTime } from '@comunica/mediatortype-time';

export class ActorHttpDelay extends ActorHttp {
  private readonly minimum: number;
  private readonly range: number;
  private readonly mediatorHttp: MediatorHttp;

  // Context key to indicate that the actor has already wrapped the given request
  private static readonly keyWrapped = new ActionContextKey<boolean>('urn:comunica:actor-http-delay#wrapped');

  public constructor(args: IActorHttpLimitRateArgs) {
    super(args);
    this.mediatorHttp = args.mediatorHttp;
    this.minimum = Math.max(args.average - args.delta, 0);
    this.range = Math.max(2 * args.delta, 0);
  }

  public async test(action: IActionHttp): Promise<TestResult<IMediatorTypeTime>> {
    if (action.context.has(ActorHttpDelay.keyWrapped)) {
      return failTest(`${this.name} can only wrap a request once`);
    }
    if (this.minimum === 0 && this.range === 0) {
      return failTest(`${this.name} has no latency range available`);
    }
    return passTest({ time: 0 });
  }

  public async run(action: IActionHttp): Promise<IActorHttpOutput> {
    const requestUrl = ActorHttp.getInputUrl(action.input);
    const requestDelay = Math.round(this.minimum + Math.random() * this.range);

    if (requestDelay > 0) {
      this.logDebug(action.context, 'Delaying request', () => ({
        url: requestUrl.href,
        delay: requestDelay,
      }));
      await new Promise(resolve => setTimeout(resolve, requestDelay));
    }

    return this.mediatorHttp.mediate({
      ...action,
      context: action.context.set(ActorHttpDelay.keyWrapped, true),
    });
  }
}

export interface IActorHttpLimitRateArgs extends IActorHttpArgs {
  /**
   * The HTTP mediator.
   */
  mediatorHttp: MediatorHttp;
  /**
   * The average delay to be applied, used as the centre of the uniform distribution.
   */
  average: number;
  /**
   * The maximum delta from the average.
   */
  delta: number;
}
