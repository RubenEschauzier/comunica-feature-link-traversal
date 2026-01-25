import type { ISourceState } from '@comunica/types';
import { CacheKey } from './CacheKey';
import { RdfStore } from 'rdf-stores';

export const CacheEntrySourceState = {
  /**
   * Cache for storing source states in a persistent manner over multiple queries
   */
  cacheSourceState: new CacheKey<
  ISourceState, ISourceState, { headers: Headers }
  >('@comunica/persistent-cache-manager:sourceState'),
};
