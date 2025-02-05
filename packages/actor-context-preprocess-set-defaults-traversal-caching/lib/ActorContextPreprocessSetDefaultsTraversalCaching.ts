import { ActorContextPreprocess, IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { KeysCaches } from '@comunica/context-entries';
import { IAction, IActorTest, passTestVoid, TestResult } from '@comunica/core';
import { ISourceState } from '@comunica/types';
import CachePolicy = require('http-cache-semantics');
import { LRUCache } from 'lru-cache';

/**
 * A comunica Set Defaults Traversal Caching Context Preprocess Actor.
 */
export class ActorContextPreprocessSetDefaultsTraversalCaching extends ActorContextPreprocess {
  private policyCache: LRUCache<string, CachePolicy>;
  private storeCache: LRUCache<string, ISourceState>;
  private readonly cacheSize: number;
  
  public constructor(args: IActorContextPreprocessSetSourceCacheArgs) {
    super(args);
    this.policyCache = new LRUCache<string, CachePolicy>({ max: this.cacheSize });
    this.storeCache = new LRUCache<string, ISourceState>({ max: this.cacheSize });

  }

  public async test(_action: IAction): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    let context = action.context;
    context = context
      .setDefault(KeysCaches.policyCache, this.policyCache )
      .setDefault(KeysCaches.storeCache, this.storeCache );
    
    return { context }
  }
}

export interface IActorContextPreprocessSetSourceCacheArgs extends IActorContextPreprocessArgs {
  /**
   * The maximum number of entries in the source cache, set to 0 to disable.
   * @range {integer}
   * @default {100}
   */
  cacheSize: number
}
