import type {
  IActionOptimizeQueryOperation,
  IActorOptimizeQueryOperationArgs,
  IActorOptimizeQueryOperationOutput,
} from '@comunica/bus-optimize-query-operation';
import {
  ActorOptimizeQueryOperation,
} from '@comunica/bus-optimize-query-operation';
import { CacheEntryDataSummary, CacheEntrySourceState } from '@comunica/cache-manager-entries';
import { KeysCaching, KeysInitQuery, KeysQuerySourceIdentify } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContextKey, passTestVoid } from '@comunica/core';
import type { ILink, ISourceState, IPersistentCache, ISetFn } from '@comunica/types';

import type * as RDF from '@rdfjs/types';
import { IDataSummary, PersistentCacheCset } from '@comunica/caches-link-traversal';

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheCsetOfflineTraversal extends ActorOptimizeQueryOperation {
  private cacheQuerySourceState: PersistentCacheCset;
  private readonly cacheSizeNumTriples: number;

  private readonly cacheDeserializationDone: Promise<void>;

  public constructor(args: IActorOptimizeQueryOperationSetCacheCsetArgs) {
    super(args);
    this.cacheSizeNumTriples = args.cacheSizeNumTriples;
    this.cacheQuerySourceState = new PersistentCacheCset(
      { maxNumSummaries: args.cacheSizeNumTriples, serializationLoc: 'temp-cache-content.json' },
    );
    this.cacheDeserializationDone = this.cacheQuerySourceState.deserialize();
    console.log(`Created cset cache with maxSize: ${args.cacheSizeNumTriples}`);
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
      this.cacheQuerySourceState = new PersistentCacheCset(
        { maxNumSummaries: this.cacheSizeNumTriples, serializationLoc: 'temp-cache-content.json' },
      );
      console.log(`Cleaned cset cache, size: ${await this.cacheQuerySourceState.size()}`);
    }

    const timeoutCallbacks = context.get(KeysInitQuery.timeoutCallbacks);
    if (timeoutCallbacks) {
      console.log('Adding serialization callback to timeout callbacks');
      timeoutCallbacks.push(async() => await this.cacheQuerySourceState.serialize());
    }

    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntryDataSummary.cacheCsetCpsSummary,
      this.cacheQuerySourceState,
      new SetCsetCacheOfflineTraversal(),
    );
    console.log(`Register: ${CacheEntryDataSummary.cacheCsetCpsSummary.id}`)

    return { context, operation: action.operation };
  }
}

export class SetCsetCacheOfflineTraversal implements 
ISetFn<
  ISourceState, ISourceState, { headers: Headers }
> {
  public async setInCache(
    key: string,
    value: ISourceState,
    cache: IPersistentCache<ISourceState, IDataSummary>,
    context: { headers: Headers },
  ): Promise<void> {
    // Retrieve the existing cached state to preserve previous traversal entries
    const cachedState = await cache.get(key);
    
    const previousLinks = cachedState?.defaultTraversal || [];
    const existingDefaultLinks = new Set<string>(previousLinks);

    for (const traverseEntry of value.metadata.traverse) {
      const traverseMetadata = traverseEntry.metadata;
      if (!traverseMetadata || !('matchingPatterns' in traverseMetadata)) {
        existingDefaultLinks.add(traverseEntry.url);
      }
    }
    
    // Attach the merged traversal list to the incoming value before saving
    value.metadata.defaultTraversal = Array.from(existingDefaultLinks);
      
    // Update the cache for this key
    cache.set(key, value);
  }
}

export interface IActorOptimizeQueryOperationSetCacheCsetArgs extends IActorOptimizeQueryOperationArgs {
  /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {10000}
   */
  cacheSizeNumTriples: number;
}

/**
 * Interface containing data for offline traversal
 */
export interface IOfflineTraversalEntry {
  /**
   * The traversal entries depending on predicates in the query
   */
  predicates: Record<string, ILink>;
  /**
   * The traversal entries independent of the query
   */
  default: ILink[];
}
