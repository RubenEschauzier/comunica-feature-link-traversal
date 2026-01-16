import type { ISourceState } from '@comunica/types';
import { ViewKey } from './ViewKey';

export const CacheSourceStateView = {
  /**
   * Cache for storing source states in a persistent manner over multiple queries
   */
  cacheSourceStateView: 
    new ViewKey<ISourceState, { url: string }, ISourceState>('@comunica/persistent-cache-manager:sourceStateView'),
};
