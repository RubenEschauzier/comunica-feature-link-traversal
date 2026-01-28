import { ActorQuerySourceIdentify, IActionQuerySourceIdentify, IActorQuerySourceIdentifyOutput, IActorQuerySourceIdentifyArgs } from '@comunica/bus-query-source-identify';
import { TestResult, IActorTest, passTestVoid, failTest } from '@comunica/core';
import { QuerySourceCache } from './QuerySourceCache';
import { KeysCaching } from '@comunica/context-entries-link-traversal';

/**
 * A comunica Cache Query Source Identify Actor.
 */
export class ActorQuerySourceIdentifyCache extends ActorQuerySourceIdentify {
  public constructor(args: IActorQuerySourceIdentifyArgs) {
    super(args);
  }

  public async test(action: IActionQuerySourceIdentify): Promise<TestResult<IActorTest>> {
    const source = action.querySourceUnidentified;
    if (source.type !== undefined && source.type !== 'cache') {
      return failTest(`${this.name} requires a single query source with cache type to be present in the context.`);
    }
    return passTestVoid();
  }

  public async run(action: IActionQuerySourceIdentify): Promise<IActorQuerySourceIdentifyOutput> {
    const querySourceContext = action.querySourceUnidentified.context!;

    return {
      querySource: {
        source: new QuerySourceCache(
          querySourceContext.getSafe(KeysCaching.cacheManager)
          action.querySourceUnidentified.cacheKey
        ),
        context: querySourceContext,
      },
    };
  }
}
