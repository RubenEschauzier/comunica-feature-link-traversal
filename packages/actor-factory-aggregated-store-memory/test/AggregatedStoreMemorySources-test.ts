import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'readable-stream';
import { AggregatedStoreMemory } from '../lib/AggregatedStoreMemory';
import 'jest-rdf';

/**
 * The documents a triple was read from, which the store keeps beside its quads so that one triple
 * asserted by several documents stays one quad.
 */
describe('AggregatedStoreMemory', () => {
  const DF = new DataFactory();
  let store: AggregatedStoreMemory;

  beforeEach(() => {
    store = new AggregatedStoreMemory(
      undefined,
      async(first, second) => ({ ...first, ...second }),
      true,
      DF,
    );
  });

  describe('source tracking', () => {
    const POD = 'http://pod.example/alice/';
    const DOC = 'http://pod.example/alice/comments/1';
    let tracking: AggregatedStoreMemory;

    const quads = () => [
      DF.quad(DF.namedNode('s1'), DF.namedNode('p1'), DF.namedNode('o1')),
      DF.quad(DF.namedNode('s1'), DF.namedNode('p2'), DF.literal('lit')),
    ];
    const importFrom = async(target: AggregatedStoreMemory, source?: string): Promise<void> => {
      const emitter = target.import(Readable.from(quads()), source);
      await new Promise(resolve => emitter.on('end', resolve));
    };

    beforeEach(() => {
      tracking = new AggregatedStoreMemory(
        undefined,
        async(first, second) => ({ ...first, ...second }),
        true,
        DF,
        true,
      );
    });

    it('records the document a quad was imported from.', async() => {
      await importFrom(tracking, DOC);

      expect(tracking.getSources(quads()[0])).toBe(DOC);
      expect(tracking.getSources(quads()[1])).toBe(DOC);
    });

    it('records nothing when no source is given.', async() => {
      await importFrom(tracking);

      expect(tracking.getSources(quads()[0])).toBeUndefined();
    });

    it('records nothing when source tracking is off.', async() => {
      await importFrom(store, DOC);

      expect(store.getSources(quads()[0])).toBeUndefined();
    });

    it('stores one quad however many documents assert the triple.', async() => {
      await importFrom(tracking, DOC);
      await importFrom(tracking, POD);

      tracking.end();
      const matched = await tracking.match().toArray();
      expect(matched).toHaveLength(2);
    });

    it('keeps the narrower name when an aggregator delivers the same triple.', async() => {
    // A derived resource is asked under the subtree it aggregates, so that URL stands for the
    // document rather than naming a second one
      await importFrom(tracking, DOC);
      await importFrom(tracking, POD);

      expect(tracking.getSources(quads()[0])).toBe(DOC);
    });

    it('keeps the narrower name whichever of the two arrives first.', async() => {
      await importFrom(tracking, POD);
      await importFrom(tracking, DOC);

      expect(tracking.getSources(quads()[0])).toBe(DOC);
    });

    it('keeps both when two documents genuinely assert the same triple.', async() => {
      const other = 'http://pod.example/bob/comments/1';
      await importFrom(tracking, DOC);
      await importFrom(tracking, other);

      expect(tracking.getSources(quads()[0])).toEqual([ DOC, other ]);
    });

    it('does not take a document for one whose name merely extends it.', async() => {
      const sibling = 'http://pod.example/alice2/comments/1';
      await importFrom(tracking, 'http://pod.example/alice');
      await importFrom(tracking, sibling);

      expect(tracking.getSources(quads()[0])).toEqual([ 'http://pod.example/alice', sibling ]);
    });

    it('tells a literal apart from an IRI reading the same.', async() => {
      const emitter = tracking.import(Readable.from([
        DF.quad(DF.namedNode('s1'), DF.namedNode('p1'), DF.namedNode('x')),
      ]), DOC);
      await new Promise(resolve => emitter.on('end', resolve));
      const second = tracking.import(Readable.from([
        DF.quad(DF.namedNode('s1'), DF.namedNode('p1'), DF.literal('x')),
      ]), POD);
      await new Promise(resolve => second.on('end', resolve));

      expect(tracking.getSources(DF.quad(DF.namedNode('s1'), DF.namedNode('p1'), DF.namedNode('x')))).toBe(DOC);
      expect(tracking.getSources(DF.quad(DF.namedNode('s1'), DF.namedNode('p1'), DF.literal('x')))).toBe(POD);
    });

    it('attaches the documents to the quads returned by match.', async() => {
      await importFrom(tracking, DOC);

      tracking.end();
      const matched = await tracking.match(DF.namedNode('s1'), DF.namedNode('p1'), null, null).toArray();
      expect(matched).toHaveLength(1);
      expect((<any> matched[0]).sources).toBe(DOC);
    });

    it('leaves quads untouched when source tracking is off.', async() => {
      await importFrom(store, DOC);

      store.end();
      const matched = await store.match(DF.namedNode('s1'), DF.namedNode('p1'), null, null).toArray();
      expect(matched).toHaveLength(1);
      expect((<any> matched[0]).sources).toBeUndefined();
    });

    it('carries the setting over to a clone.', async() => {
      const cloned = <AggregatedStoreMemory> tracking.clone();
      await importFrom(cloned, DOC);

      expect(cloned.getSources(quads()[0])).toBe(DOC);
    });
  });
});
