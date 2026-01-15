import type { IActionQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import type { ICacheKey } from '@comunica/cache-manager-entries';
import type { ILink, IQuerySource, MetadataBindings, ICachePolicy } from '@comunica/types';

// TODO: Think about how to set / get in cache without having to go into comunica default. I would prefer
// to keep this self-contained. Possibly through the use of wrapper around new get links bus. With
// a way of preventing caching by the within query cache.
// TODO: This currently ties the view function to a specific cache implementation. Instead
// what we should do is tie the view function to a token, with one cache having multiple possible
// 'view' tokens (see gemini). This way other parts of the engine just need access to this view token
// nothing else.
// TODO Then using this view we can also reimplement our sort-cache-cardinality by just getting the
// view token, issueing a query to the cache view and getting results. Super CLEAN!

export class PersistentCacheManager {
  protected registry = new Map<string, ICacheRegistryEntry>();
  /**
   * Registers a cache and its set method to the manager. This cache can then be added to
   * using a call to the set strategy with the associated cacheId and the setStrategy.
   * @param cacheKey the cache to set to
   * @param cache the cache being registered
   * @param setFn the function called when a new value is added to the cache
   */
  public registerCache<T, C>(
    cacheKey: ICacheKey<T, C>,
    cache: any,
    setFn: ISetFn<T, C>,
  ): void {
    if (this.registry.has(cacheKey.id)) {
      // TODO: Add some warning or something when this happens?
      return;
    }
    this.registry.set(cacheKey.id, { cache, setFn });
  }

  /**
   * Sets a cache using a specified setting strategy defined
   * when the cache was registered
   * @param cacheKey the cache to set to
   * @param key the string value key for the value to be set,
   * can be augmented in the setFn using data from the context
   * @param value the value to be set
   * @param context any additional context required for the setFn
   */
  public setCache<T, C>(
    cacheKey: ICacheKey<T, C>,
    key: string,
    value: T,
    context: C,
  ): void {
    const { cache, setFn } = this.ensureCache(cacheKey.id);
    setFn.setInCache(key, value, cache, context);
  }

  public getFromCache<T, C>(
    cacheKey: ICacheKey<T, C>,
    view: ICacheView<T, C>,
    context: C,
  ): T {
    const relevantCache = this.ensureCache(cacheKey.id);
    return view.construct(relevantCache, context);
  }

  protected ensureCache(cacheId: string): ICacheRegistryEntry {
    const relevantCache = this.registry.get(cacheId);
    if (!relevantCache) {
      throw new Error('Tried to set or get from a cache that was never registered');
    }
    return relevantCache;
  }
}

export interface IPersistentCache {
  // TODO: Implement generic wrapper interface for getting and setting caches, so any arbitrary
  // cache can be put into this.
}

/**
 * NOTE: This is taken from '@comunica/actor-query-source-identify-hypermedia'
 * as its not exported and I don't want to deal with changing comunica and linking
 * for something so small.
 * The current state of a source.
 * This is needed for following links within a source.
 */
export interface ISourceState {
  /**
   * The link to this source.
   */
  link: ILink;
  /**
   * A source.
   */
  source: IQuerySource;
  /**
   * The source's initial metadata.
   */
  metadata: MetadataBindings;
  /**
   * All dataset identifiers that have been passed for this source.
   */
  handledDatasets: Record<string, boolean>;
  /**
   * The cache policy of the request's response.
   */
  cachePolicy?: ICachePolicy<IActionQuerySourceDereferenceLink>;
}

/**
 * Interface of class that sets a value in a given cache for a given key. This can
 * be a simple URL -> ISourceState mapping or URL -> Source Data Summary. The
 * computation and logic of what to store is done in the setInCache function.
 * T = The type of data being stored (e.g., ISourceState)
 * C = The context needed to derive additional storage logic
 */
export interface ISetFn<T, C> {
  setInCache: (
    key: string,
    value: T,
    cache: any,
    context: C
  ) => void;
}

/**
 * Interface for a class that returns a view over the cache. This can be
 * just retrieving a key, a summary of the content of the cache, or
 * the content of a cache matching a certain operator (e.g. triple pattern).
 * T the output type of the constructed view
 * C the context needed to construct the view
 */
export interface ICacheView<T, C> {
  construct: (cache: any, context: C) => T;
}

export interface ICacheRegistryEntry {
  cache: any;
  setFn: ISetFn<any, any>;
}
