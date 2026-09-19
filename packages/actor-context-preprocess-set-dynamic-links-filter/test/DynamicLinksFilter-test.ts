import { DynamicFilter } from '../lib/DynamicLinksFilter';

const POD = 'http://solidbench-server:3000/pods/00000000000000000933';
const OTHER_POD = 'http://solidbench-server:3000/pods/000000000000000009331';

describe('DynamicFilter', () => {
  let filter: DynamicFilter;

  beforeEach(() => {
    filter = new DynamicFilter();
  });

  it('matches nothing while empty.', () => {
    expect(filter.matchesFilter(`${POD}/posts/2010-02-14`)).toBe(false);
  });

  describe('addExact', () => {
    it('matches the url it was given, and only that url.', () => {
      filter.addExact(`${POD}/.meta`);

      expect(filter.matchesFilter(`${POD}/.meta`)).toBe(true);
      expect(filter.matchesFilter(`${POD}/.meta/`)).toBe(false);
      expect(filter.matchesFilter(`${POD}/posts/2010-02-14`)).toBe(false);
    });
  });

  describe('addGlob', () => {
    it('covers every document below a subtree.', () => {
      filter.addGlob(`${POD}/**/*`);

      expect(filter.matchesFilter(`${POD}/profile/card`)).toBe(true);
      expect(filter.matchesFilter(`${POD}/posts/2010-02-14`)).toBe(true);
      expect(filter.matchesFilter(`${POD}/comments/2010/02/14/deep/document`)).toBe(true);
    });

    it('covers dot-prefixed documents below a subtree.', () => {
      // A subtree is only registered once the derived resources of its pod are identified, so the
      // .meta that advertises them has already served its purpose
      filter.addGlob(`${POD}/**/*`);

      expect(filter.matchesFilter(`${POD}/.meta`)).toBe(true);
      expect(filter.matchesFilter(`${POD}/profile/.meta`)).toBe(true);
      expect(filter.matchesFilter(`${POD}/profile/.acl`)).toBe(true);
    });

    it('leaves the documents of other pods alone.', () => {
      filter.addGlob(`${POD}/**/*`);

      expect(filter.matchesFilter(`${OTHER_POD}/.meta`)).toBe(false);
      expect(filter.matchesFilter(`${OTHER_POD}/posts/2010-02-14`)).toBe(false);
      expect(filter.matchesFilter('http://solidbench-server:3000/www.ldbc.eu/vocabulary')).toBe(false);
    });

    it('does not let a subtree root match a pod whose name merely extends it.', () => {
      filter.addGlob(`${POD}/**/*`);

      // OTHER_POD is POD with one more character, so a prefix test without the separator
      // would swallow the whole of it
      expect(filter.matchesFilter(`${OTHER_POD}/profile/card`)).toBe(false);
    });

    it('covers the subtree root itself, whichever selector shape registered it.', () => {
      // As a glob, `<dir>/**/*` holds one segment less than `<dir>/**` and so leaves the container
      // out. The container lists the documents just declared covered, so crawling it buys nothing
      // and both shapes are treated alike
      for (const selector of [ `${POD}/**`, `${POD}/**/*` ]) {
        const covered = new DynamicFilter();
        covered.addGlob(selector);

        expect(covered.matchesFilter(`${POD}/`)).toBe(true);
        expect(covered.matchesFilter(POD)).toBe(true);
        expect(covered.matchesFilter(`${POD}/posts/2010-02-14`)).toBe(true);
      }
    });

    it('strips the serialization extension off a selector.', () => {
      filter.addGlob(`${POD}/**/*.nq`);

      expect(filter.matchesFilter(`${POD}/posts/2010-02-14`)).toBe(true);
      expect(filter.matchesFilter(`${POD}/posts/2010-02-14.ttl`)).toBe(true);
    });

    it('covers nested subtrees independently.', () => {
      filter.addGlob(`${POD}/posts/**/*`);

      expect(filter.matchesFilter(`${POD}/posts/2010-02-14`)).toBe(true);
      expect(filter.matchesFilter(`${POD}/profile/card`)).toBe(false);
    });

    it('matches urls carrying a fragment or a query string.', () => {
      filter.addGlob(`${POD}/**/*`);

      expect(filter.matchesFilter(`${POD}/profile/card#me`)).toBe(true);
      expect(filter.matchesFilter(`${POD}/posts/2010-02-14?page=2`)).toBe(true);
    });

    it('ignores the casing of the authority.', () => {
      filter.addGlob('http://SolidBench-Server:3000/pods/1/**/*');

      expect(filter.matchesFilter('http://solidbench-server:3000/pods/1/profile/card')).toBe(true);
    });

    it('is unaffected by adding the same selector twice.', () => {
      filter.addGlob(`${POD}/**/*`);
      filter.addGlob(`${POD}/**/*`);

      expect(filter.matchesFilter(`${POD}/profile/card`)).toBe(true);
      expect(filter.matchesFilter(`${OTHER_POD}/profile/card`)).toBe(false);
    });
  });

  describe('addGlob with a selector that is narrower than a subtree', () => {
    it('matches a single level of documents without descending.', () => {
      filter.addGlob(`${POD}/posts/*`);

      expect(filter.matchesFilter(`${POD}/posts/2010-02-14`)).toBe(true);
      expect(filter.matchesFilter(`${POD}/posts/2010/02/14`)).toBe(false);
      expect(filter.matchesFilter(`${POD}/profile/card`)).toBe(false);
    });

    it('matches dot-prefixed documents, as a subtree does.', () => {
      filter.addGlob(`${POD}/posts/*`);

      expect(filter.matchesFilter(`${POD}/posts/.meta`)).toBe(true);
    });

    it('matches a wildcard in the middle of a path.', () => {
      filter.addGlob(`${POD}/*/card`);

      expect(filter.matchesFilter(`${POD}/profile/card`)).toBe(true);
      expect(filter.matchesFilter(`${POD}/profile/other`)).toBe(false);
    });

    it('handles a selector that starts with a wildcard.', () => {
      filter.addGlob('**/*');

      expect(filter.matchesFilter(`${POD}/profile/card`)).toBe(true);
    });

    it('matches a selector holding no wildcard at all exactly.', () => {
      filter.addGlob(`${POD}/profile/card`);

      expect(filter.matchesFilter(`${POD}/profile/card`)).toBe(true);
      expect(filter.matchesFilter(`${POD}/profile/card2`)).toBe(false);
    });
  });

  describe('with many pods registered', () => {
    it('answers each pod for its own documents only.', () => {
      const pods = Array.from(
        { length: 200 },
        (_, index) => `http://solidbench-server:3000/pods/${String(index).padStart(20, '0')}`,
      );
      for (const pod of pods) {
        filter.addGlob(`${pod}/**/*.nq`);
      }

      for (const pod of pods) {
        expect(filter.matchesFilter(`${pod}/posts/2010-02-14`)).toBe(true);
      }
      expect(filter.matchesFilter('http://solidbench-server:3000/pods/unregistered/posts/x')).toBe(false);
      expect(filter.matchesFilter('http://other-server:3000/pods/00000000000000000000/posts/x')).toBe(false);
    });
  });
});
