import type { ICacheKey } from '@comunica/cache-manager-entries';
import type { IViewKey } from '@comunica/cache-manager-entries/lib/ViewKey';
import type { ICacheRegistryEntry, ICacheView, IPersistentCache, ISetFn } from '@comunica/types-link-traversal';

// TODO: Think about how to set / get in cache without having to go into comunica default. I would prefer
// to keep this self-contained. Possibly through the use of wrapper around new get links bus. With
// a way of preventing caching by the within query cache.
// TODO: Test caching
// TODO Fix endpoint stopping when query times out
// TODO Then using this view we can also reimplement our sort-cache-cardinality by just getting the
// view token, issueing a query to the cache view and getting results. Super CLEAN!
// TODO Reimplement caching performance tracking in nice way.

export class PersistentCacheManager {
  protected cacheRegistry = new Map<string, ICacheRegistryEntry<any, any, any>>();
  protected viewRegistry = new Map<string, ICacheView<any, any, any>>();

  /**
   * Registers a cache and its set method to the manager. This cache can then be added to
   * using a call to the set strategy with the associated cacheId and the setStrategy.
   * @param cacheKey the cache to set to
   * @param cache the cache being registered
   * @param setFn the function called when a new value is added to the cache
   */
  public registerCache<I, S, C>(
    cacheKey: ICacheKey<I, S, C>,
    cache: IPersistentCache<S>,
    setFn: ISetFn<I, S, C>,
  ): void {
    if (this.cacheRegistry.has(cacheKey.id)) {
      return;
    }
    this.cacheRegistry.set(cacheKey.id, { cache, setFn });
  }

  /**
   * Registers a view over a cache
   * @param viewKey The key the view will be available over
   * @param view The function used to create the view
   * @returns
   */
  public registerCacheView<T, C, K>(
    viewKey: IViewKey<T, C, K>,
    view: ICacheView<T, C, K>,
  ) {
    if (this.viewRegistry.has(viewKey.id)) {
      return;
    }
    this.viewRegistry.set(viewKey.id, view);
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
  public async setCache<I, S, C>(
    cacheKey: ICacheKey<I, S, C>,
    key: string,
    value: I,
    context: C,
  ): Promise<void> {
    const { cache, setFn } = this.ensureCache(cacheKey);
    return setFn.setInCache(key, value, cache, context);
  }

  /**
   * Gets from cache. Here the actual setting type and context type for the cache key
   * are not needed as this is only relevant to the setting function.
   * @param cacheKey The key of the associated cache. Can be any of the keys
   * for setting functions that share the same cache.
   * @param viewKey The key of the view required by the caller
   * @param context Any context needed to execute the the view
   * @returns Result of the view applied over the given cache
   */
  public async getFromCache<S, C, K>(
    cacheKey: ICacheKey<unknown, S, unknown>,
    viewKey: IViewKey<S, C, K>,
    context: C,
  ): Promise<K | undefined> {
    const relevantCache = this.ensureCache(cacheKey);
    const view = this.ensureView(viewKey);

    return view.construct(relevantCache.cache, context);
  }

  public getRegisteredCaches() {
    return this.cacheRegistry;
  }

  public getRegisteredViews() {
    return this.viewRegistry;
  }

  protected ensureCache<I, S, C>(cacheKey: ICacheKey<I, S, C>): ICacheRegistryEntry<I, S, C> {
    const relevantCache = this.cacheRegistry.get(cacheKey.id);
    if (!relevantCache) {
      throw new Error('Tried to set or get from a cache that was never registered');
    }
    return relevantCache;
  }

  protected ensureView<S, C, K>(viewKey: IViewKey<S, C, K>): ICacheView<S, C, K> {
    const view = this.viewRegistry.get(viewKey.id);
    if (!view) {
      throw new Error(`Tried to get a cache view that was never registered`);
    }
    return view;
  }
}
