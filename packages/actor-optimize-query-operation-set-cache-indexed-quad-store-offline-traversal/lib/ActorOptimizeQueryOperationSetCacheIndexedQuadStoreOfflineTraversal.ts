import type {
  IActionOptimizeQueryOperation,
  IActorOptimizeQueryOperationArgs,
  IActorOptimizeQueryOperationOutput,
} from '@comunica/bus-optimize-query-operation';
import {
  ActorOptimizeQueryOperation,
} from '@comunica/bus-optimize-query-operation';
import { CacheEntrySourceState } from '@comunica/cache-manager-entries';
import { PersistentCacheSourceStateIndexedQuadStore } from '@comunica/caches-link-traversal';
import { KeysCaching, KeysInitQuery, KeysQuerySourceIdentify } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContextKey, passTestVoid } from '@comunica/core';
import type { ISourceState, IPersistentCache, ISetFn } from '@comunica/types';

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheIndexedQuadStoreOfflineTraversal extends ActorOptimizeQueryOperation {
  private cacheQuerySourceState: PersistentCacheSourceStateIndexedQuadStore;
  private readonly cacheSizeDiskNumTriples: number;
  private readonly cacheDeserializationDone: Promise<void>;

  public constructor(args: IActorOptimizeQueryOperationSetCacheIndexedQuadStoreOfflineTraversalArgs) {
    super(args);
    this.cacheSizeDiskNumTriples = args.cacheSizeNumTriples;

    this.cacheQuerySourceState = new PersistentCacheSourceStateIndexedQuadStore(
      {
        maxNumTriples: this.cacheSizeDiskNumTriples,
      },
    );
    this.cacheDeserializationDone = this.cacheQuerySourceState.deserialize();
    console.log(`Created cache with max size: ${this.cacheSizeDiskNumTriples} triples`);
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    await this.cacheDeserializationDone;

    const context = action.context;
    if (!action.context.get(KeysQuerySourceIdentify.traverse)) {
      return { context, operation: action.operation };
    }

    if (context.get(KeysCaching.clearCache) || context.get(new ActionContextKey('clearCache'))) {
      this.cacheQuerySourceState = new PersistentCacheSourceStateIndexedQuadStore(
        {
          maxNumTriples: this.cacheSizeDiskNumTriples,
        },
      );
      await this.cacheQuerySourceState.clear();
      console.log(`Cleaned cache, size: ${await this.cacheQuerySourceState.size()}`);
    }

    const timeoutCallbacks = context.get(KeysInitQuery.timeoutCallbacks);
    if (timeoutCallbacks) {
      console.log('Adding serialization callback to timeout callbacks');
      timeoutCallbacks.push(async() => await this.cacheQuerySourceState.serialize());
    }

    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceStateIndexedQuadStore,
      this.cacheQuerySourceState,
      new SetSourceStateCacheOfflineTraversalQuadStore(),
    );
    return { context, operation: action.operation };
  }
}

export class SetSourceStateCacheOfflineTraversalQuadStore implements ISetFn<ISourceState, ISourceState, { headers: Headers }> {
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
    value.metadata.defaultTraversal = [ ...existingDefaultLinks ];
    cache.set(key, value);
  }
}

export interface IActorOptimizeQueryOperationSetCacheIndexedQuadStoreOfflineTraversalArgs extends IActorOptimizeQueryOperationArgs {
  /**
   * The maximum number of triples in the disk cache.
   * @range {integer}
   * @default {1240000}
   */
  cacheSizeNumTriples: number;
}
