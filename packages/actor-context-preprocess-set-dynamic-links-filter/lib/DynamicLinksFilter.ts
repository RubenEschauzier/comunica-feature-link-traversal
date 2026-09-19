import type { IDynamicFilter } from '@comunica/types-link-traversal';
import { Minimatch } from 'minimatch';
import { SegmentedUriTrieFilter } from './SegmentedUriTrieFilter';

/**
 * Decides which links traversal no longer has to dereference.
 *
 * Entries are only ever added, never removed, so a url that matches once matches forever.
 *
 * The globs come from derived resource selectors, so one is added per pod traversal walks, while
 * `matchesFilter` runs on every link popped from the queue. Testing every glob against every link
 * is therefore quadratic in the size of the crawl, which is why the two shapes that actually occur
 * are answered without a glob engine at all:
 *  - an exact url is a set lookup;
 *  - `<dir>/**` and `<dir>/**\/*` cover a whole subtree, which the uri trie answers in the depth of
 *    the url rather than the number of registered directories.
 * Whatever is narrower than that keeps minimatch, but is compiled once instead of on every call,
 * which is where nearly all of the cost of a `minimatch(url, glob)` sits.
 */
export class DynamicFilter implements IDynamicFilter {
  private readonly exact = new Set<string>();
  /**
   * The directories covered in full. The trie parses urls rather than comparing them as strings,
   * so query strings, fragments and authority casing do not decide whether a pod is covered.
   */
  private readonly subtrees = new SegmentedUriTrieFilter<true>();
  private hasSubtrees = false;
  /**
   * The globs that cover less than a subtree, compiled once and kept by their source text.
   */
  private readonly patterns = new Map<string, Minimatch>();

  public addExact(exactMatch: string): void {
    this.exact.add(exactMatch);
  }

  public addGlob(selector: string): void {
    const glob = selector.replace(/\.[^./*]+$/u, '');

    // `<dir>/**` and `<dir>/**\/*` hold everything below `<dir>`, which is a subtree rather than a
    // glob. The two differ over whether `<dir>` itself is held, but `<dir>` is the container
    // listing the very documents just declared covered, so there is nothing left to crawl there
    // either and both are registered the same way
    const subtree = /^([^*?[{]*)\/\*\*(?:\/\*)?$/u.exec(glob);
    if (subtree && subtree[1].length > 0) {
      this.subtrees.addDomainRule(subtree[1], true);
      this.hasSubtrees = true;
      return;
    }

    if (!this.patterns.has(glob)) {
      // Dot-prefixed documents are covered like any other, see matchesFilter
      this.patterns.set(glob, new Minimatch(glob, { dot: true }));
    }
  }

  /**
   * Whether traversal can skip this url.
   *
   * A covered subtree includes its dot-prefixed documents, which is where this departs from a
   * plain glob: minimatch only matches those under `dot: true`. A subtree is registered once the
   * derived resources of its pod have been identified, and the `.meta` describing them is the
   * reason to dereference it, so there is nothing left to fetch there. Pods that have not been
   * visited yet live under their own subtrees and are still reached.
   */
  public matchesFilter(url: string): boolean {
    if (this.exact.has(url)) {
      return true;
    }
    if (this.hasSubtrees && this.subtrees.hasFilterMatching(url)) {
      return true;
    }
    for (const pattern of this.patterns.values()) {
      // eslint-disable-next-line unicorn/prefer-regexp-test -- Minimatch#match, not String#match
      if (pattern.match(url)) {
        return true;
      }
    }
    return false;
  }
}
