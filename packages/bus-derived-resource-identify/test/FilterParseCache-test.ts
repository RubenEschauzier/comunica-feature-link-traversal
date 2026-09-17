import { FilterParseCache } from '../lib/FilterParseCache';

describe('FilterParseCache', () => {
  let cache: FilterParseCache;

  beforeEach(() => {
    cache = new FilterParseCache();
  });

  describe('key', () => {
    it('separates different queries, base IRIs and formats.', () => {
      const format = { language: 'sparql', version: '1.1' };
      expect(FilterParseCache.key('SELECT * WHERE { ?s ?p ?o }'))
        .not.toBe(FilterParseCache.key('SELECT * WHERE { ?a ?b ?c }'));
      expect(FilterParseCache.key('q', format, 'http://a.example/'))
        .not.toBe(FilterParseCache.key('q', format, 'http://b.example/'));
      expect(FilterParseCache.key('q', { language: 'graphql', version: '1.1' }))
        .not.toBe(FilterParseCache.key('q', format));
    });

    it('is stable for the same inputs.', () => {
      const format = { language: 'sparql', version: '1.1' };
      expect(FilterParseCache.key('q', format, 'http://a.example/'))
        .toBe(FilterParseCache.key('q', format, 'http://a.example/'));
    });
  });

  describe('parse', () => {
    it('parses once for repeated calls with the same key.', async() => {
      const parse = jest.fn(async() => ({ operation: 'parsed' }));

      const first = await cache.parse('k', parse);
      const second = await cache.parse('k', parse);

      expect(parse).toHaveBeenCalledTimes(1);
      // The same object is handed out, which is why callers must not mutate it
      expect(second).toBe(first);
    });

    it('parses separately for different keys.', async() => {
      const parse = jest.fn(async() => ({ operation: 'parsed' }));
      await cache.parse('k1', parse);
      await cache.parse('k2', parse);
      expect(parse).toHaveBeenCalledTimes(2);
    });

    it('shares one parse between concurrent callers.', async() => {
      let release: () => void;
      const blocked = new Promise<void>((resolve) => {
        release = resolve;
      });
      const parse = jest.fn(async() => {
        await blocked;
        return { operation: 'parsed' };
      });

      const pending = [ cache.parse('k', parse), cache.parse('k', parse), cache.parse('k', parse) ];
      release!();
      const results = await Promise.all(pending);

      expect(parse).toHaveBeenCalledTimes(1);
      expect(results[0]).toBe(results[1]);
      expect(results[1]).toBe(results[2]);
    });

    it('does not cache a failed parse.', async() => {
      const failing = jest.fn(async() => {
        throw new Error('bad filter');
      });
      await expect(cache.parse('k', failing)).rejects.toThrow('bad filter');

      const succeeding = jest.fn(async() => ({ operation: 'parsed' }));
      await expect(cache.parse('k', succeeding)).resolves.toEqual({ operation: 'parsed' });
      expect(succeeding).toHaveBeenCalledTimes(1);
    });

    it('does not share entries between instances.', async() => {
      // Two actors, or two tests, must never see each other's parses
      const other = new FilterParseCache();
      const parse = jest.fn(async() => ({ operation: 'parsed' }));

      await cache.parse('k', parse);
      await other.parse('k', parse);

      expect(parse).toHaveBeenCalledTimes(2);
    });

    it('re-parses after clear.', async() => {
      const parse = jest.fn(async() => ({ operation: 'parsed' }));
      await cache.parse('k', parse);
      cache.clear();
      await cache.parse('k', parse);
      expect(parse).toHaveBeenCalledTimes(2);
    });
  });
});
