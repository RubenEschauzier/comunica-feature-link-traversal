export interface ICompositeRule {
  /**
   * The canonical namespace prefix representing the composite resource domain.
   * e.g., "https://pod.example/alice/"
   */
  readonly domainPrefix: string;

  /**
   * The variable names that must reside within `domainPrefix`.
   * - Star pattern: ['s']
   * - Linear 2-hop: ['s', 'o1'] (terminal ?o2 is omitted)
   */
  readonly requiredAuthoritativeVars: string[];
}

export interface IParsedUri {
  scheme: string;
  authority: string;
  path: string;
}

export class TrieNode<T = ICompositeRule> {
  public readonly value: string;
  public isTerminal: boolean;
  public readonly rules: T[] = [];
  public readonly children: Map<string, TrieNode<T>> = new Map();

  public constructor(value: string, isTerminal = false) {
    this.value = value;
    this.isTerminal = isTerminal;
  }

  public addChild(child: TrieNode<T>): void {
    this.children.set(child.value, child);
  }

  public getChild(value: string): TrieNode<T> | undefined {
    return this.children.get(value);
  }
}

/**
 * Answers whether a uri falls under a registered prefix, in the depth of the uri rather than the
 * number of prefixes registered. Uris are split on authority and path segment rather than compared
 * as strings, so a query string, a fragment or the casing of the authority does not decide the
 * answer, and `/api` never matches `/apigateway`.
 */
export class SegmentedUriTrieFilter<T = ICompositeRule> {
  protected readonly authorities: Map<string, TrieNode<T>> = new Map();

  public constructor() {}

  /**
   * Registers a domain prefix rule associated with a composite resource.
   */
  public addDomainRule(filterUri: string, rule: T): void {
    const { authority, path } = SegmentedUriTrieFilter.parseUri(filterUri);

    let currentNode: TrieNode<T> | undefined = this.authorities.get(authority);
    if (!currentNode) {
      currentNode = new TrieNode<T>(authority, false);
      this.authorities.set(authority, currentNode);
    }

    const len = path.length;
    let startIdx = path.codePointAt(0) === 47 /* '/' */ ? 1 : 0;

    // Rule applies to the root authority (e.g., "https://example.org/")
    if (startIdx >= len) {
      currentNode.isTerminal = true;
      currentNode.rules.push(rule);
      return;
    }

    // Narrowed to strictly non-undefined for the traversal loop
    let activeNode: TrieNode<T> = currentNode;

    while (startIdx < len) {
      let endIdx = path.indexOf('/', startIdx);
      const isLastSegment = endIdx === -1 || endIdx === len - 1;

      if (endIdx === -1) {
        endIdx = len;
      }

      if (endIdx > startIdx) {
        const segment = path.slice(startIdx, endIdx);
        let child: TrieNode<T> | undefined = activeNode.getChild(segment);

        if (!child) {
          child = new TrieNode<T>(segment, isLastSegment);
          activeNode.addChild(child);
        } else if (isLastSegment) {
          child.isTerminal = true;
        }

        if (isLastSegment) {
          child.rules.push(rule);
        }

        // Child is guaranteed to be TrieNode<T> here
        activeNode = child;
      }

      startIdx = endIdx + 1;
    }
  }

  /**
   * Resolves all composite rules that claim authority over this document source URI.
   * Collects rules hierarchically from root to deepest prefix.
   */
  public getAllMatchingRules(uri: string): T[] {
    const { authority, path } = SegmentedUriTrieFilter.parseUri(uri);

    let currentNode: TrieNode<T> | undefined = this.authorities.get(authority);
    if (!currentNode) {
      return [];
    }

    let results: T[] = [];

    // Collect root authority rules if present
    if (currentNode.rules.length > 0) {
      results = [ ...results, ...currentNode.rules ];
    }

    const len = path.length;
    let startIdx = path.codePointAt(0) === 47 /* '/' */ ? 1 : 0;

    while (startIdx < len && currentNode !== undefined) {
      let endIdx = path.indexOf('/', startIdx);
      if (endIdx === -1) {
        endIdx = len;
      }

      if (endIdx > startIdx) {
        const segment = path.slice(startIdx, endIdx);
        const nextNode: TrieNode<T> | undefined = currentNode.getChild(segment);
        if (!nextNode) {
          break;
        }
        currentNode = nextNode;

        if (currentNode.rules.length > 0) {
          results = [ ...results, ...currentNode.rules ];
        }
      }

      startIdx = endIdx + 1;
    }

    return results;
  }

  /**
   * Fast boolean check: returns true if any terminal node covers this URI.
   */
  public hasFilterMatching(uri: string): boolean {
    const { authority, path } = SegmentedUriTrieFilter.parseUri(uri);

    let currentNode: TrieNode<T> | undefined = this.authorities.get(authority);
    if (!currentNode) {
      return false;
    }

    const len = path.length;
    let startIdx = path.codePointAt(0) === 47 /* '/' */ ? 1 : 0;

    if (startIdx >= len) {
      return Boolean(currentNode.isTerminal);
    }

    while (startIdx < len && currentNode !== undefined) {
      if (currentNode.isTerminal) {
        return true;
      }

      let endIdx = path.indexOf('/', startIdx);
      if (endIdx === -1) {
        endIdx = len;
      }

      if (endIdx > startIdx) {
        const segment = path.slice(startIdx, endIdx);
        const nextNode: TrieNode<T> | undefined = currentNode.getChild(segment);
        if (!nextNode) {
          return false;
        }
        currentNode = nextNode;
      }

      startIdx = endIdx + 1;
    }

    return Boolean(currentNode?.isTerminal);
  }

  /**
   * Validates whether a given term URI belongs strictly to a specified prefix domain.
   */
  public matchesPrefix(termUri: string, prefixDomain: string): boolean {
    const parsedTerm = SegmentedUriTrieFilter.parseUri(termUri);
    const parsedPrefix = SegmentedUriTrieFilter.parseUri(prefixDomain);

    if (parsedTerm.authority !== parsedPrefix.authority) {
      return false;
    }

    if (!parsedTerm.path.startsWith(parsedPrefix.path)) {
      return false;
    }

    // Boundary check: prevent /api matching /apigateway
    if (
      parsedPrefix.path.endsWith('/') ||
      parsedTerm.path.length === parsedPrefix.path.length ||
      parsedTerm.path.charAt(parsedPrefix.path.length) === '/'
    ) {
      return true;
    }

    return false;
  }

  public static parseUri(uri: string): IParsedUri {
    const splitIndex = uri.indexOf('://');
    const scheme = splitIndex === -1 ? '' : uri.slice(0, splitIndex).toLowerCase();
    const rest = splitIndex === -1 ? uri : uri.slice(splitIndex + 3);

    const authorityPathSplit = rest.indexOf('/');
    if (authorityPathSplit === -1) {
      return {
        scheme,
        authority: this.normalizeAuthority(rest),
        path: '/',
      };
    }

    const authority = rest.slice(0, authorityPathSplit);
    let path = rest.slice(authorityPathSplit);

    const indexQuery = path.indexOf('?');
    if (indexQuery > -1) {
      path = path.slice(0, indexQuery);
    }
    const indexFragment = path.indexOf('#');
    if (indexFragment > -1) {
      path = path.slice(0, indexFragment);
    }

    return {
      scheme,
      authority: this.normalizeAuthority(authority),
      path,
    };
  }

  public static normalizeAuthority(authority: string): string {
    const atIndex = authority.indexOf('@');
    return atIndex === -1 ?
      authority.toLowerCase() :
      authority.slice(0, atIndex + 1) + authority.slice(atIndex + 1).toLowerCase();
  }
}
