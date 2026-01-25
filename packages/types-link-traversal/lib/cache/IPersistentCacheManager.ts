import type { AsyncIterator } from 'asynciterator';

export interface IPersistentCache<T> {
  get: (key: string) => Promise<T | undefined>;
  getMany: (keys: string[]) => Promise<(T | undefined)[]>;
  set: (key: string, value: T) => Promise<void>;
  has: (key: string) => Promise<boolean>;
  delete: (key: string) => Promise<boolean>;
  entries: () => AsyncIterator<[string, T]>;
  serialize: () => Promise<void>;
}

/**
 * Interface of class that sets a value in a given cache for a given key. This can
 * be a simple URL -> ISourceState mapping or URL -> Source Data Summary. The
 * computation and logic of what to store is done in the setInCache function.
 * T = The type of data being stored (e.g., ISourceState)
 * C = The context needed to derive additional storage logic
 */
export interface ISetFn<I, S, C> {
  setInCache: (
    key: string,
    value: I,
    cache: IPersistentCache<S>,
    context: C
  ) => Promise<void>;
}

/**
 * Interface for a class that returns a view over the cache. This can be
 * just retrieving a key, a summary of the content of the cache, or
 * the content of a cache matching a certain operator (e.g. triple pattern).
 * T the output type of the constructed view
 * C the context needed to construct the view
 */
export interface ICacheView<S, C, K> {
  construct: (cache: IPersistentCache<S>, context: C) => Promise<K | undefined>;
}

export interface ICacheRegistryEntry<I, S, C> {
  cache: IPersistentCache<S>;
  setFn: ISetFn<I, S, C>;
}
