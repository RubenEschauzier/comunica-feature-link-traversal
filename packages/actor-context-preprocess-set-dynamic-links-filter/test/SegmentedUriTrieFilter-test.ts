import {
  SegmentedUriTrieFilter,
  type IParsedUri,
  type ICompositeRule,
} from '../lib/SegmentedUriTrieFilter';

class TestableSegmentedUriTrieFilter extends SegmentedUriTrieFilter<ICompositeRule> {
  // ParseUri is static on the filter, this keeps the tests reading as instance calls
  public parseUri(uri: string): IParsedUri {
    return SegmentedUriTrieFilter.parseUri(uri);
  }
}

describe('SegmentedUriTrieFilter', () => {
  let filter: TestableSegmentedUriTrieFilter;

  beforeEach(() => {
    filter = new TestableSegmentedUriTrieFilter();
  });

  describe('An parseUri invocation', () => {
    describe('for standard extraction', () => {
      it('should split a standard HTTP/HTTPS URI into scheme, authority, and path', () => {
        const result = filter.parseUri('https://example.com/api/v1/users');
        expect(result).toEqual({
          scheme: 'https',
          authority: 'example.com',
          path: '/api/v1/users',
        });
      });

      it('should default path to "/" when no path is provided', () => {
        const result = filter.parseUri('https://example.com');
        expect(result).toEqual({
          scheme: 'https',
          authority: 'example.com',
          path: '/',
        });
      });

      it('should handle custom protocol schemes', () => {
        const result = filter.parseUri('grpc://internal.service/rpc.Endpoint');
        expect(result).toEqual({
          scheme: 'grpc',
          authority: 'internal.service',
          path: '/rpc.Endpoint',
        });
      });
    });

    describe('for RFC 3986 authority handling and segment stripping', () => {
      it('should strip query parameters from the path', () => {
        const result = filter.parseUri('https://example.com/search?query=jest&page=1');
        expect(result).toEqual({
          scheme: 'https',
          authority: 'example.com',
          path: '/search',
        });
      });

      it('should strip URL fragment hashes from the path', () => {
        const result = filter.parseUri('https://example.com/docs#overview');
        expect(result).toEqual({
          scheme: 'https',
          authority: 'example.com',
          path: '/docs',
        });
      });

      it('should preserve non-default ports in authority', () => {
        const result = filter.parseUri('http://localhost:8080/dashboard');
        expect(result).toEqual({
          scheme: 'http',
          authority: 'localhost:8080',
          path: '/dashboard',
        });
      });

      it('should preserve default ports verbatim under RFC 3986 syntax rules', () => {
        const result = filter.parseUri('https://cdn.example.org:443/assets/img/');
        expect(result).toEqual({
          scheme: 'https',
          authority: 'cdn.example.org:443',
          path: '/assets/img/',
        });
      });

      it('should preserve userinfo in the authority', () => {
        const result = filter.parseUri('https://admin:secret123@secure.internal/admin');
        expect(result).toEqual({
          scheme: 'https',
          authority: 'admin:secret123@secure.internal',
          path: '/admin',
        });
      });

      it('should preserve full authority with userinfo and port while stripping query and fragment', () => {
        const result = filter.parseUri(
          'https://user:pass@api.test.io:8443/v2/items?filter=active&limit=10#section',
        );
        expect(result).toEqual({
          scheme: 'https',
          authority: 'user:pass@api.test.io:8443',
          path: '/v2/items',
        });
      });

      it('should preserve authority consisting of an IPv6 host and port', () => {
        const result = filter.parseUri('http://[::1]:9000/metrics');
        expect(result).toEqual({
          scheme: 'http',
          authority: '[::1]:9000',
          path: '/metrics',
        });
      });
    });

    describe('for casing and edge cases', () => {
      it('should normalize scheme and host to lowercase while preserving userinfo and path case', () => {
        const result = filter.parseUri('HTTPS://User:Secret@MyDomain.ORG:8080/UserData/Profile');
        expect(result).toEqual({
          scheme: 'https',
          authority: 'User:Secret@mydomain.org:8080',
          path: '/UserData/Profile',
        });
      });

      it('should preserve trailing slashes in the path', () => {
        const result = filter.parseUri('https://example.com/static/');
        expect(result.path).toBe('/static/');
      });

      it('should handle URIs without a scheme prefix', () => {
        const result = filter.parseUri('example.org/api/v1');
        expect(result).toEqual({
          scheme: '',
          authority: 'example.org',
          path: '/api/v1',
        });
      });

      it('should handle URIs without scheme and path', () => {
        const result = filter.parseUri('example.org');
        expect(result).toEqual({
          scheme: '',
          authority: 'example.org',
          path: '/',
        });
      });
    });
  });

  describe('An hasFilterMatching invocation', () => {
    it('should return false for any URI when the trie is empty', () => {
      expect(filter.hasFilterMatching('https://example.com/api')).toBe(false);
      expect(filter.hasFilterMatching('http://localhost:8080/')).toBe(false);
    });

    describe('for exact and disjoint URI matches', () => {
      it('should return true for an exact URI registered in the trie', () => {
        filter.addDomainRule('https://example.com/api/v1/users', {
          domainPrefix: 'https://example.com/api/v1/users',
          requiredAuthoritativeVars: [ 's' ],
        });

        expect(filter.hasFilterMatching('https://example.com/api/v1/users')).toBe(true);
      });

      it('should return false for URIs matching on path but belonging to a different authority', () => {
        filter.addDomainRule('https://example.com/api/v1/users', {
          domainPrefix: 'https://example.com/api/v1/users',
          requiredAuthoritativeVars: [ 's' ],
        });

        expect(filter.hasFilterMatching('https://other.org/api/v1/users')).toBe(false);
      });

      it('should return false for parent paths of an inserted specific filter', () => {
        filter.addDomainRule('https://example.com/api/v1/users', {
          domainPrefix: 'https://example.com/api/v1/users',
          requiredAuthoritativeVars: [ 's' ],
        });

        expect(filter.hasFilterMatching('https://example.com/api/v1')).toBe(false);
        expect(filter.hasFilterMatching('https://example.com/api')).toBe(false);
        expect(filter.hasFilterMatching('https://example.com/')).toBe(false);
      });

      it('should match root-level path filters', () => {
        filter.addDomainRule('https://example.com/', {
          domainPrefix: 'https://example.com/',
          requiredAuthoritativeVars: [ 's' ],
        });

        expect(filter.hasFilterMatching('https://example.com/')).toBe(true);
        expect(filter.hasFilterMatching('https://example.com')).toBe(true);
      });
    });

    describe('for hierarchical and prefix paths', () => {
      it('should return true for deeper sub-paths when a prefix filter is registered', () => {
        filter.addDomainRule('https://example.com/api', {
          domainPrefix: 'https://example.com/api',
          requiredAuthoritativeVars: [ 's' ],
        });

        expect(filter.hasFilterMatching('https://example.com/api')).toBe(true);
        expect(filter.hasFilterMatching('https://example.com/api/v1')).toBe(true);
        expect(filter.hasFilterMatching('https://example.com/api/v1/users/123')).toBe(true);
      });

      it('should not match partial string prefixes across path boundaries', () => {
        filter.addDomainRule('https://example.com/api', {
          domainPrefix: 'https://example.com/api',
          requiredAuthoritativeVars: [ 's' ],
        });

        expect(filter.hasFilterMatching('https://example.com/apigateway')).toBe(false);
      });

      it('should distinguish multiple branching paths on the same authority', () => {
        filter.addDomainRule('https://example.com/api/v1', {
          domainPrefix: 'https://example.com/api/v1',
          requiredAuthoritativeVars: [ 's' ],
        });
        filter.addDomainRule('https://example.com/static/images', {
          domainPrefix: 'https://example.com/static/images',
          requiredAuthoritativeVars: [ 's' ],
        });

        expect(filter.hasFilterMatching('https://example.com/api/v1')).toBe(true);
        expect(filter.hasFilterMatching('https://example.com/api/v1/data')).toBe(true);
        expect(filter.hasFilterMatching('https://example.com/static/images')).toBe(true);
        expect(filter.hasFilterMatching('https://example.com/static/images/logo.png')).toBe(true);

        expect(filter.hasFilterMatching('https://example.com/api/v2')).toBe(false);
        expect(filter.hasFilterMatching('https://example.com/static/css')).toBe(false);
      });
    });
  });

  describe('An getAllMatchingRules invocation', () => {
    it('should return an empty array when no rules match the URI', () => {
      filter.addDomainRule('https://example.com/api', {
        domainPrefix: 'https://example.com/api',
        requiredAuthoritativeVars: [ 's' ],
      });

      expect(filter.getAllMatchingRules('https://example.com/other')).toEqual([]);
      expect(filter.getAllMatchingRules('https://other.com/api')).toEqual([]);
    });

    describe('for single and multiple rule associations', () => {
      it('should return rules registered directly at the matched node', () => {
        const rule: ICompositeRule = {
          domainPrefix: 'https://example.com/api/v1',
          requiredAuthoritativeVars: [ 's' ],
        };
        filter.addDomainRule('https://example.com/api/v1', rule);

        const results = filter.getAllMatchingRules('https://example.com/api/v1/users');
        expect(results).toEqual([ rule ]);
      });

      it('should accumulate multiple rules registered on the exact same domain path', () => {
        const starRule: ICompositeRule = {
          domainPrefix: 'https://example.com/api',
          requiredAuthoritativeVars: [ 's' ],
        };
        const linearRule: ICompositeRule = {
          domainPrefix: 'https://example.com/api',
          requiredAuthoritativeVars: [ 's', 'o1' ],
        };

        filter.addDomainRule('https://example.com/api', starRule);
        filter.addDomainRule('https://example.com/api', linearRule);

        const results = filter.getAllMatchingRules('https://example.com/api/items');
        expect(results).toHaveLength(2);
        expect(results).toContain(starRule);
        expect(results).toContain(linearRule);
      });
    });

    describe('for hierarchical inheritance', () => {
      it('should inherit rules from root authority down to deep prefix paths', () => {
        const rootRule: ICompositeRule = {
          domainPrefix: 'https://example.com/',
          requiredAuthoritativeVars: [ 's' ],
        };
        const subRule: ICompositeRule = {
          domainPrefix: 'https://example.com/pods/alice/',
          requiredAuthoritativeVars: [ 's', 'o1' ],
        };

        filter.addDomainRule('https://example.com/', rootRule);
        filter.addDomainRule('https://example.com/pods/alice/', subRule);

        const matchedRootOnly = filter.getAllMatchingRules('https://example.com/public/data.ttl');
        expect(matchedRootOnly).toEqual([ rootRule ]);

        const matchedBoth = filter.getAllMatchingRules('https://example.com/pods/alice/profile.ttl');
        expect(matchedBoth).toHaveLength(2);
        expect(matchedBoth).toEqual([ rootRule, subRule ]);
      });
    });
  });

  describe('An matchesPrefix invocation', () => {
    it('should return true for identical URIs', () => {
      expect(
        filter.matchesPrefix('https://example.com/api/v1', 'https://example.com/api/v1'),
      ).toBe(true);
    });

    it('should return true for sub-paths within the prefix domain', () => {
      expect(
        filter.matchesPrefix('https://example.com/api/v1/users', 'https://example.com/api/v1'),
      ).toBe(true);
      expect(
        filter.matchesPrefix('https://example.com/api/v1/users/42', 'https://example.com/api/v1/'),
      ).toBe(true);
    });

    it('should return false for different authorities', () => {
      expect(
        filter.matchesPrefix('https://other.com/api/v1/users', 'https://example.com/api/v1'),
      ).toBe(false);
    });

    it('should return false when the path is not a sub-path', () => {
      expect(
        filter.matchesPrefix('https://example.com/api', 'https://example.com/api/v1'),
      ).toBe(false);
      expect(
        filter.matchesPrefix('https://example.com/other', 'https://example.com/api'),
      ).toBe(false);
    });

    it('should enforce path boundaries to prevent partial segment matches', () => {
      expect(
        filter.matchesPrefix('https://example.com/apigateway', 'https://example.com/api'),
      ).toBe(false);
      expect(
        filter.matchesPrefix('https://example.com/api-v2', 'https://example.com/api'),
      ).toBe(false);
    });
  });
});
