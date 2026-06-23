import type {
  IActionOptimizeQueryOperation,
  IActorOptimizeQueryOperationArgs,
  IActorOptimizeQueryOperationOutput,
} from '@comunica/bus-optimize-query-operation';
import {
  ActorOptimizeQueryOperation,
} from '@comunica/bus-optimize-query-operation';
import { CacheEntrySourceState } from '@comunica/cache-manager-entries';
import { KeysCaching, KeysInitQuery, KeysQuerySourceIdentify } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContextKey, failTest, passTestVoid } from '@comunica/core';
import type { ISourceState, IPersistentCache, ISetFn } from '@comunica/types';

import { PersistentCacheIndexedDisk } from '@comunica/caches-link-traversal';

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheIndexedDiskOfflineTraversal extends ActorOptimizeQueryOperation {
  private readonly cacheSizeDiskNumTriples: number;
  private readonly cacheSizeHotNumTriples: number;
  private readonly hotCachePolicy: 'lru' | 'lru-filtered';

  private cacheQuerySourceState: PersistentCacheIndexedDisk | undefined;
  private cacheDeserializationDone: Promise<void> | undefined;

  public constructor(args: IActorOptimizeQueryOperationSetCacheIndexedDiskOfflineTraversalArgs) {
    super(args);
    this.cacheSizeDiskNumTriples = args.cacheSizeDiskNumTriples;
    this.cacheSizeHotNumTriples = args.cacheSizeHotNumTriples;
    this.hotCachePolicy = args.hotCachePolicy;

    // this.cacheQuerySourceState = new PersistentCacheIndexedDisk(
    //   {
    //     maxNumTriples: this.cacheSizeDiskNumTriples,
    //     maxTriplesInMemory: this.cacheSizeHotNumTriples,
    //     hotCachePolicy: this.hotCachePolicy,
    //   },
    // );
    // this.cacheDeserializationDone = this.cacheQuerySourceState.deserialize();
    console.log(`Created cache with max on disk/hot size: ${this.cacheSizeDiskNumTriples}/${this.cacheSizeHotNumTriples}`);
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    if (action.context.get(KeysInitQuery.isMaster)){
      return failTest(`${this.name} only creates cache for worker threads`);
    }
    return passTestVoid();
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    if (!this.cacheQuerySourceState){
      this.createCache(true);
    }
    await this.cacheDeserializationDone;

    const context = action.context;
    if (!action.context.get(KeysQuerySourceIdentify.traverse)) {
      return { context, operation: action.operation };
    }

    if (context.get(KeysCaching.clearCache) || context.get(new ActionContextKey('clearCache'))) {
      this.createCache(false);
      await this.cacheQuerySourceState!.clear();
      console.log(`Cleaned cache, size: ${await this.cacheQuerySourceState!.size()}`);
    }

    const timeoutCallbacks = context.get(KeysInitQuery.timeoutCallbacks);
    if (timeoutCallbacks) {
      console.log('Adding serialization callback to timeout callbacks');
      timeoutCallbacks.push(async() => await this.cacheQuerySourceState!.serialize());
    }

    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceStateIndexedDisk,
      this.cacheQuerySourceState!,
      new SetSourceStateCacheOfflineTraversalDisk(),
    );
    return { context, operation: action.operation };
  }
  
  protected createCache(deserialize: boolean){
    this.cacheQuerySourceState = new PersistentCacheIndexedDisk(
      {
        maxNumTriples: this.cacheSizeDiskNumTriples,
        maxTriplesInMemory: this.cacheSizeHotNumTriples,
        hotCachePolicy: this.hotCachePolicy,
      },
    );
    if (deserialize){
      this.cacheDeserializationDone = this.cacheQuerySourceState.deserialize();
    }
  }
}

export class SetSourceStateCacheOfflineTraversalDisk implements ISetFn<ISourceState, ISourceState, { headers: Headers }> {
  public async setInCache(
    key: string,
    value: ISourceState,
    cache: IPersistentCache<ISourceState, ISourceState>,
    context: { headers: Headers },
  ): Promise<void> {
    // Retrieve the existing cached state to preserve previous traversal entries
    const cachedState = await cache.get(key);
    
    const previousLinks = cachedState?.metadata.defaultTraversal || [];
    const existingDefaultLinks = new Set<string>(previousLinks);

    for (const traverseEntry of value.metadata.traverse) {
      const traverseMetadata = traverseEntry.metadata;
      if (!traverseMetadata || !('matchingPatterns' in traverseMetadata)) {
        existingDefaultLinks.add(traverseEntry.url);
      }
    }
    
    // Attach the merged traversal list to the incoming value before saving
    value.metadata.defaultTraversal = Array.from(existingDefaultLinks);
    cache.set(key, value);
  }
}

export interface IActorOptimizeQueryOperationSetCacheIndexedDiskOfflineTraversalArgs extends IActorOptimizeQueryOperationArgs {
  /**
   * The maximum number of triples in the disk cache.
   * @range {integer}
   * @default {1240000}
   */
  cacheSizeDiskNumTriples: number;
  /**
   * The maximum number of triples in the hot cache.
   * @range {integer}
   * @default {124000}
   */
  cacheSizeHotNumTriples: number;
  /**
   * Policy to use for cache. Filtered LRU first requires the document
   * to be used twice in a given window of 10000 links
   */
  hotCachePolicy: 'lru' | 'lru-filtered';
}
