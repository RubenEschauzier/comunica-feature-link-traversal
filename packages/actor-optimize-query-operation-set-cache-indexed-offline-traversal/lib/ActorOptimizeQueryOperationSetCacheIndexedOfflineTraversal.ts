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
import { ActionContextKey, passTestVoid } from '@comunica/core';
import type { ISourceState, IPersistentCache, ISetFn } from '@comunica/types';

import type { IOfflineTraversalEntry } from '@comunica/types-link-traversal';
import type * as RDF from '@rdfjs/types';
import { PersistentCacheSourceStateIndexed } from '@comunica/caches-link-traversal';

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheIndexedOfflineTraversal extends ActorOptimizeQueryOperation {
  private cacheQuerySourceState: PersistentCacheSourceStateIndexed;
  private readonly cacheSizeNumTriples: number;

  private readonly cacheDeserializationDone: Promise<void>;

  public constructor(args: IActorOptimizeQueryOperationSetCacheIndexedOfflineTraversalArgs) {
    super(args);
    this.cacheSizeNumTriples = args.cacheSizeNumTriples;
    this.cacheQuerySourceState = new PersistentCacheSourceStateIndexed(
      { maxNumTriples: args.cacheSizeNumTriples, serializationLoc: 'temp-cache-content.json' },
    );
    this.cacheDeserializationDone = this.cacheQuerySourceState.deserialize();
    console.log(`Created indexed cache with maxSize: ${args.cacheSizeNumTriples}`);
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
      this.cacheQuerySourceState = new PersistentCacheSourceStateIndexed(
        { maxNumTriples: this.cacheSizeNumTriples, serializationLoc: 'temp-cache-content.json' },
      );
      console.log(`Cleaned cache, size: ${await this.cacheQuerySourceState.size()}`);
    }

    const timeoutCallbacks = context.get(KeysInitQuery.timeoutCallbacks);
    if (timeoutCallbacks) {
      console.log('Adding serialization callback to timeout callbacks');
      timeoutCallbacks.push(async() => await this.cacheQuerySourceState.serialize());
    }

    // TODO: This still ties the implementation of the cache to the setter.
    // True modularity would be to have a cache package with different cache implementations,
    // cache set views and cache get views in actors.
    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceStateIndexed,
      this.cacheQuerySourceState,
      new SetSourceStateCacheOfflineTraversal(),
    );

    return { context, operation: action.operation };
  }
}

export class SetSourceStateCacheOfflineTraversal implements ISetFn<ISourceState, ISourceState, { headers: Headers }> {
  public async setInCache(
    key: string,
    value: ISourceState,
    cache: IPersistentCache<ISourceState, ISourceState>,
    context: { headers: Headers },
  ): Promise<void> {
    
    // Retrieve the existing cached state to preserve previous traversal entries
    const cachedState = await cache.get(key);
    

    // Initialize with existing cached traversal data, or create a fresh object
    const traversalAdjList: IOfflineTraversalEntry = cachedState?.metadata.offlineTraversal ?? {
      predicates: {},
      default: [],
    };

    for (const traverseEntry of value.metadata.traverse) {
      const traverseMetadata = traverseEntry.metadata;
      
      if (traverseMetadata && 'matchingPatterns' in traverseMetadata) {
        // Check if cMatch batched criterion produced this
        for (const quad of (<RDF.BaseQuad[]> traverseMetadata.matchingPatterns)) {
          traversalAdjList.predicates[quad.predicate.value] = { url: traverseEntry.url };
        }
      } else {
        // Append to default array only if the URL is not already present
        const exists = traversalAdjList.default.some(link => link.url === traverseEntry.url);
        if (!exists) {
          traversalAdjList.default.push({ url: traverseEntry.url });
        }
      }
    }
    
    // Attach the merged traversal list to the incoming value before saving
    value.metadata.offlineTraversal = traversalAdjList;
    
    // Update the cache for this key
    cache.set(key, value);
  }
}

export interface IActorOptimizeQueryOperationSetCacheIndexedOfflineTraversalArgs extends IActorOptimizeQueryOperationArgs {
  /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124000}
   */
  cacheSizeNumTriples: number;
}
