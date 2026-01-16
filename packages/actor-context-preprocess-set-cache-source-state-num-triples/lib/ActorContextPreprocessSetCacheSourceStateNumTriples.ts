// TODO THIS SHOULD BE A @comunica/TYPES IMPORT (the ISourceState one)
import type { ICacheView, ISetFn } from '@comunica/actor-context-preprocess-set-persistent-cache-manager';
import type {
  IActionContextPreprocess,
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { CacheEntrySourceState } from '@comunica/cache-manager-entries/lib';
import { CacheSourceStateView } from '@comunica/cache-manager-entries/lib/ViewKeys';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type { ISourceState } from '@comunica/types';

import { LRUCache } from 'lru-cache';
import { QuerySourceFileLazy } from '../../actor-query-source-identify-hypermedia-none-lazy/lib/QuerySourceFileLazy';

/**
 * A comunica Set Defaults Traversal Caching Context Preprocess Actor.
 */
export class ActorContextPreprocessSetDefaultsTraversalCachingNumTriples extends ActorContextPreprocess {
  private readonly cacheSourceState: LRUCache<string, ISourceState>;

  public constructor(args: IActorContextPreprocessSetSourceCacheNumTriplesArgs) {
    super(args);
    this.cacheSourceState = new LRUCache<string, ISourceState>({
      maxSize: args.cacheSizeNumTriples,
      sizeCalculation: ActorContextPreprocessSetDefaultsTraversalCachingNumTriples.getSizeSource,
    });
  }

  public async test(_action: IAction): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    const context = action.context;
    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceState,
      this.cacheSourceState,
      new SetSourceStateCache(),
    );
    cacheManager.registerCacheView(
      CacheSourceStateView.cacheSourceStateView,
      new GetSourceStateCacheView()
    )
    return { context };
  }

  /**
   * Get number of triples in store as size
   * @param source
   * @returns Size of source in number of triples
   */
  protected static getSizeSource(source: ISourceState, _key: string): number {
    // TODO: Make this a default size + eventual correctness by attaching a callback
    // to the stream in the source that counts the number of triples.
    // see: https://gemini.google.com/app/d34fbf8122906f34 
    // this requires me to also define the cache interface that all cache
    // should adhere to
    return 1;
  }
}

export class SetSourceStateCache implements ISetFn<ISourceState, Record<string, any>> {
  public setInCache(key: string, value: ISourceState, cache: any, _context: Record<string, any>): void {
    cache.set(key, value);
  }
}
export class GetSourceStateCacheView 
implements ICacheView<ISourceState, { url: string }, ISourceState> {
  public construct(cache: any, context: { url: string }): ISourceState | undefined {
    return cache.get(context.url);
  }
}

export interface IActorContextPreprocessSetSourceCacheNumTriplesArgs extends IActorContextPreprocessArgs {
  /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124_000}
   */
  cacheSizeNumTriples: number;
}
