import type { ISourceState } from '@comunica/actor-context-preprocess-set-persistent-cache-manager';
import { CacheKey } from './CacheKey';

export const CacheEntrySourceState = {
  /**
   * Cache for storing source states in a persistent manner over multiple queries
   */
  cacheSourceState: new CacheKey<ISourceState, { url: string }>('@comunica/persistent-cache-manager:sourceState'),
};
