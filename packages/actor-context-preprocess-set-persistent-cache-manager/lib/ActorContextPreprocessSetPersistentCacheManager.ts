import type { IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { PersistentCacheManager } from './PersistentCacheManager';

/**
 * A comunica Set Persistent Cache Manager Link Traversal Context Preprocess Actor.
 */
export class ActorContextPreprocessSetPersistentCacheManager extends ActorContextPreprocess {
  protected persistentCacheManager: PersistentCacheManager;
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
    this.persistentCacheManager = new PersistentCacheManager();
  }

  public async test(_action: IAction): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IAction): Promise<IActorContextPreprocessOutput> {
    let context = action.context;

    // Add new cache manager object to context if it doesn't exist
    if (!context.has(KeysCaching.cacheManager)) {
      context = context.set(KeysCaching.cacheManager, new PersistentCacheManager());
    }

    return { context };
  }
}
