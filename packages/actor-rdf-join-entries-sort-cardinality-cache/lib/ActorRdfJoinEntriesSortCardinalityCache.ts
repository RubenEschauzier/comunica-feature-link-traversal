import {
  ActorRdfJoinEntriesSort,
} from '@comunica/bus-rdf-join-entries-sort';
import type {
  IActionRdfJoinEntriesSort,
  IActorRdfJoinEntriesSortArgs,
  IActorRdfJoinEntriesSortOutput,
  IActorRdfJoinEntriesSortTest,
} from '@comunica/bus-rdf-join-entries-sort';
import { CacheKey, ICacheKey, IViewKey, ViewKey } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries';
import type { TestResult } from '@comunica/core';
import { failTest, passTest } from '@comunica/core';
import type { Algebra } from '@comunica/utils-algebra';

/**
 * An actor that sorts join entries by increasing cardinality.
 * This actor requires a cache with exposed count view and sufficient size
 * before it runs.
 */
export class ActorRdfJoinEntriesSortCardinalityCache extends ActorRdfJoinEntriesSort {
  protected readonly cacheEntryKey: ICacheKey<unknown, unknown, unknown>;
  protected readonly minCacheSize: number;

  public constructor(args: IActorRdfJoinEntriesSortCardinalityCacheArgs) {
    super(args);
    this.cacheEntryKey = new CacheKey(args.cacheEntryName);
    this.minCacheSize = args.minCacheSize;
  }

  public async test(action: IActionRdfJoinEntriesSort): Promise<TestResult<IActorRdfJoinEntriesSortTest>> {
    const cacheManager = action.context.get(KeysCaching.cacheManager);
    if (!cacheManager){
      return failTest(`${this.name} requires a cacheManager object in context`);
    }
    const registeredCache = cacheManager.getRegisteredCache(this.cacheEntryKey)
    if (!registeredCache){
      return failTest(`${this.name} cacheManager did not have passed cacheKey registered`);
    }
    const cacheSize = await registeredCache.cache.size();
    if (cacheSize < this.minCacheSize){
      return failTest(`${this.name} cache calculated size (${cacheSize}) smaller than minimal size: ${this.minCacheSize}`);
    }
    return passTest({
      accuracy: action.entries.length === 0 ?
        1 :
        action.entries
          .reduce((sum, entry) => sum + (Number.isFinite(entry.metadata.cardinality.value) ? 1 : 0), 0) /
        action.entries.length,
    });
  }

  public async run(action: IActionRdfJoinEntriesSort): Promise<IActorRdfJoinEntriesSortOutput> {
    const entries = [ ...action.entries ]
      .sort((entryLeft, entryRight) => entryLeft.metadata.cardinality.value - entryRight.metadata.cardinality.value);
    return { entries };
  }
}

export interface IActorRdfJoinEntriesSortCardinalityCacheArgs extends IActorRdfJoinEntriesSortArgs {
  /**
   * The name of the key that will be used by ActorQuerySourceIdentifyLinkTraversal to obtain the cache
   */
  cacheEntryName: string;
  /**
   * The name of the key that will be used by ActorQuerySourceIdentifyLinkTraversal to query
   * the cache for cardinality counts
   */
  cacheCountViewName: string
  /**
   * Minimal calculated size of the cache before this actor will be invoked (default is in number of triples)
   * @range {integer}
   * @default {20000}
   */
  minCacheSize: number;

}