import { ActorQuerySourceIdentify, IActionQuerySourceIdentify, IActorQuerySourceIdentifyOutput, IActorQuerySourceIdentifyArgs } from '@comunica/bus-query-source-identify';
import { TestResult, IActorTest, passTestVoid, failTest, ActionContext } from '@comunica/core';
import { QuerySourceCache } from './QuerySourceCache';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import { IQuerySourceCache } from '@comunica/types';

/**
 * A comunica Cache Query Source Identify Actor.
 */
export class ActorQuerySourceIdentifyCache extends ActorQuerySourceIdentify {
  public constructor(args: IActorQuerySourceIdentifyArgs) {
    super(args);
  }

  public async test(action: IActionQuerySourceIdentify): Promise<TestResult<IActorTest>> {
    const source = action.querySourceUnidentified;
    if (source.type === undefined || source.type !== 'cache') {
      return failTest(`${this.name} requires a single query source with cache type to be present in the context.`);
    }
    return passTestVoid();
  }

  public async run(action: IActionQuerySourceIdentify): Promise<IActorQuerySourceIdentifyOutput> {
    const querySource = <IQuerySourceCache> action.querySourceUnidentified;
    const querySourceContext = querySource.context!;

    return {
      querySource: {
        source: new QuerySourceCache(
          querySourceContext.getSafe(KeysCaching.cacheManager),
          querySource.cacheKey,
          querySource.getSource,
        ),
        context: ActionContext.ensureActionContext(querySourceContext),
      },
    };
  }
}
