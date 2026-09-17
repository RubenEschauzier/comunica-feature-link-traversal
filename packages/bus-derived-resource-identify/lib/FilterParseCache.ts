import { LRUCache } from 'lru-cache';

/**
 * Remembers parsed derived-resource filters for one actor.
 *
 * A pod advertises one derived resource per template, and an identify actor parses the filter of
 * each one in its `test` to decide whether it can serve it. The filters are template files,
 * byte-identical on every pod, so past the first pod every one of those parses repeats work
 * already done. Link traversal walks many pods, so the cost is paid per pod visited.
 *
 * Held per actor rather than per process. A process-wide cache would be shared by actors wired to
 * different parse mediators - separate engines in one process, or one test after another - and
 * would hand them each other's results. Cross-pod reuse is where the saving is; sharing between
 * the three parsing actors would save one parse per filter per process, which is not worth that.
 */
export class FilterParseCache {
  private readonly parsed: LRUCache<string, Promise<unknown>>;

  public constructor(max = 250) {
    this.parsed = new LRUCache<string, Promise<unknown>>({ max });
  }

  /**
   * Builds the key for a filter parse. Every input the parse depends on has to appear here, as two
   * filters sharing a key would silently hand one caller the other's algebra. It also makes the
   * cache self-invalidating: an edited filter is different text, and so a different key.
   */
  public static key(
    query: string,
    queryFormat?: { language: string; version: string },
    baseIRI?: string,
  ): string {
    return `${queryFormat?.language ?? ''}|${queryFormat?.version ?? ''}|${baseIRI ?? ''}|${query}`;
  }

  /**
   * Returns the parsed filter for the given key, parsing only on the first request.
   *
   * The promise is stored rather than the result, so concurrent identify calls for the same filter
   * - the normal case, since the resources of a pod are identified together - share one parse
   * instead of racing to repeat it. A rejected parse is evicted so a later attempt is not stuck
   * with the failure.
   *
   * Callers must treat the returned algebra as read-only: it is shared, where before each caller
   * held its own copy.
   */
  public async parse<T>(key: string, parse: () => Promise<T>): Promise<T> {
    const cached = this.parsed.get(key);
    if (cached) {
      return <Promise<T>> cached;
    }
    const pending = parse();
    this.parsed.set(key, pending);
    pending.catch((): void => {
      // Only drop it if it is still the entry we put there, so a newer attempt is not evicted
      if (this.parsed.get(key) === pending) {
        this.parsed.delete(key);
      }
    });
    return pending;
  }

  /**
   * Empties the cache. Filters are content-keyed, so nothing invalidates; this is for tests.
   */
  public clear(): void {
    this.parsed.clear();
  }
}
