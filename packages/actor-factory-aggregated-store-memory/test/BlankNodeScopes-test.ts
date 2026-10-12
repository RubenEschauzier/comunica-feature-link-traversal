import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'readable-stream';
import { AggregatedStoreMemory } from '../lib/AggregatedStoreMemory';
import { BlankNodeScopes } from '../lib/BlankNodeScopes';
import 'jest-rdf';

/**
 * A blank node read from several documents and derived resources of one pod, each parsed with labels
 * of its own, has to end up as one node.
 */
describe('BlankNodeScopes', () => {
  const DF = new DataFactory();
  const POD = 'http://pod.example/alice/';
  const CARD = 'http://pod.example/alice/profile/card';
  const OTHER_POD = 'http://pod.example/alice2/';
  let scopes: BlankNodeScopes;

  beforeEach(() => {
    scopes = new BlankNodeScopes(DF);
  });

  describe('scopeOf', () => {
    it('finds no scope when none was declared.', () => {
      expect(scopes.scopeOf(CARD)).toBeUndefined();
    });

    it('finds the scope a url lies in.', () => {
      scopes.addScope(POD);

      expect(scopes.scopeOf(CARD)).toBe(POD);
      expect(scopes.scopeOf(`${CARD}#me`)).toBe(POD);
      expect(scopes.scopeOf(POD)).toBe(POD);
    });

    it('does not take a pod for one whose name merely extends it.', () => {
      scopes.addScope(POD);

      expect(scopes.scopeOf(`${OTHER_POD}profile/card`)).toBeUndefined();
    });

    it('prefers the innermost scope.', () => {
      scopes.addScope('http://pod.example/');
      scopes.addScope(POD);

      expect(scopes.scopeOf(CARD)).toBe(POD);
    });
  });

  describe('within a scope', () => {
    beforeEach(() => {
      scopes.addScope(POD);
    });

    it('gives a stored label the term the first document parsed it to.', () => {
      const parsed = scopes.forParsed(DF.blankNode('b4_b1_knows12'), CARD, POD);

      expect(scopes.forParsed(DF.blankNode('b9_b1_knows12'), `${POD}posts/1`, POD)).toBe(parsed);
      expect(scopes.forStored('b1_knows12', POD, POD)).toBe(parsed);
    });

    it('keeps different labels apart.', () => {
      const first = scopes.forParsed(DF.blankNode('b4_b1_knows12'), CARD, POD);

      expect(scopes.forParsed(DF.blankNode('b4_b1_knows13'), CARD, POD)).not.toEqual(first);
    });

    it('gives a label first seen in a resource a term of its own, which documents then share.', () => {
      const stored = scopes.forStored('b1_knows12', POD, POD);

      expect(stored.value).not.toBe('b1_knows12');
      expect(scopes.forParsed(DF.blankNode('b4_b1_knows12'), CARD, POD)).toBe(stored);
    });

    it('keeps the same label in another pod apart.', () => {
      scopes.addScope(OTHER_POD);
      const mine = scopes.forStored('b1_knows12', POD, POD);

      expect(scopes.forStored('b1_knows12', OTHER_POD, OTHER_POD)).not.toEqual(mine);
    });
  });

  describe('outside of a scope', () => {
    it('keeps the blank nodes of two documents apart.', () => {
      const card = scopes.forParsed(DF.blankNode('b4_b1_knows12'), CARD, undefined);

      expect(scopes.forParsed(DF.blankNode('b9_b1_knows12'), `${POD}posts/1`, undefined)).not.toEqual(card);
    });

    it('finds a document read before its scope was declared once it is.', () => {
      const card = scopes.forParsed(DF.blankNode('b4_b1_knows12'), CARD, undefined);
      scopes.addScope(POD);

      expect(scopes.forStored('b1_knows12', POD, scopes.scopeOf(POD))).toBe(card);
    });
  });

  describe('in the aggregated store', () => {
    const KNOWS = DF.namedNode('http://example.org/knows');
    const DATE = DF.namedNode('http://example.org/date');
    let store: AggregatedStoreMemory;

    const importFrom = async(quads: any[], source: string): Promise<void> => {
      const emitter = store.import(Readable.from(quads), source);
      await new Promise(resolve => emitter.on('end', resolve));
    };

    beforeEach(() => {
      store = new AggregatedStoreMemory(undefined, async(first, second) => ({ ...first, ...second }), true, DF, true);
    });

    it('stores a triple read from a document and from a derived resource of its pod once.', async() => {
      await importFrom([ DF.quad(DF.namedNode(`${CARD}#me`), KNOWS, DF.blankNode('b4_b1_knows12')) ], CARD);
      store.addBlankNodeScope(POD);
      await importFrom([ DF.quad(DF.namedNode(`${CARD}#me`), KNOWS, DF.blankNode('b7_b1_knows12')) ], POD);

      expect(store.getStore().countQuads(null, KNOWS, null, null)).toBe(1);
    });

    it('joins triples about one blank node read from different parses of its pod.', async() => {
      store.addBlankNodeScope(POD);
      await importFrom([ DF.quad(DF.namedNode(`${CARD}#me`), KNOWS, DF.blankNode('b4_b1_knows12')) ], CARD);
      await importFrom([ DF.quad(DF.blankNode('b7_b1_knows12'), DATE, DF.literal('2010')) ], POD);

      const [ knows ] = store.getStore().getQuads(null, KNOWS, null, null);
      const [ date ] = store.getStore().getQuads(null, DATE, null, null);
      expect(date.subject).toEqualRdfTerm(knows.object);
      expect(store.scopeStoredBlankNode('b1_knows12', POD)).toEqualRdfTerm(knows.object);
    });

    it('leaves the blank nodes of documents outside any scope apart.', async() => {
      await importFrom([ DF.quad(DF.namedNode(`${CARD}#me`), KNOWS, DF.blankNode('b4_b1_knows12')) ], CARD);
      await importFrom([ DF.quad(DF.namedNode(`${CARD}#me`), KNOWS, DF.blankNode('b7_b1_knows12')) ], `${POD}posts/1`);

      expect(store.getStore().countQuads(null, KNOWS, null, null)).toBe(2);
    });
  });
});
