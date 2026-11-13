import { KeysInitQuery } from '@comunica/context-entries';
import { ActionContext } from '@comunica/core';
import type { IActionContext, IQuerySource } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import { ArrayIterator, BufferedIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { QuerySourceExtractLinks } from '../lib/QuerySourceExtractLinks';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);
const AF = new AlgebraFactory(DF);
const URL = 'http://ex.org/shop/sparql';

/**
 * A mediator that reads the quads it is given and answers with one link per subject.
 */
function linkMediator(seen: RDF.Quad[]): any {
  return {
    mediate: jest.fn(async({ metadata }: any) => new Promise((resolve) => {
      metadata.on('data', (quad: RDF.Quad) => seen.push(quad));
      metadata.on('end', () => resolve({ links: seen.map(quad => ({ url: quad.subject.value })) }));
    })),
  };
}

function manager(started = true): any {
  return {
    started,
    addDereferencingDerivedResource: jest.fn(),
    completeDereferencingDerivedResource: jest.fn(),
    removeDereferencingDerivedResource: jest.fn(),
    addLinks: jest.fn(async() => {}),
  };
}

async function flush(): Promise<void> {
  for (let i = 0; i < 5; i++) {
    await new Promise(resolve => setImmediate(resolve));
  }
}

describe('QuerySourceExtractLinks', () => {
  let context: IActionContext;
  let inner: IQuerySource;
  let rows: RDF.Bindings[];

  beforeEach(() => {
    context = new ActionContext({ [KeysInitQuery.dataFactory.name]: DF });
    rows = [
      BF.bindings([[ DF.variable('review'), DF.namedNode('http://ex.org/r1') ], [ DF.variable('reviewer'), DF.namedNode('http://ex.org/p1') ]]),
      BF.bindings([[ DF.variable('review'), DF.namedNode('http://ex.org/r2') ]]),
    ];
    inner = <any> {
      referenceValue: URL,
      getFilterFactor: jest.fn(async() => 1),
      getSelectorShape: jest.fn(async() => 'shape'),
      queryBindings: jest.fn(() => {
        const stream = new ArrayIterator(rows, { autoStart: false });
        stream.setProperty('metadata', { cardinality: 2 });
        return stream;
      }),
      queryQuads: jest.fn(() => 'quads'),
      queryBoolean: jest.fn(async() => true),
      queryVoid: jest.fn(async() => {}),
      toString: () => 'inner',
    };
  });

  describe('delegating', () => {
    it('passes everything but bindings on to the inner source', async() => {
      const source = new QuerySourceExtractLinks(inner, URL, linkMediator([]), manager(), DF);
      const ask = AF.createAsk(AF.createNop());
      expect(source.referenceValue).toBe(URL);
      await expect(source.getFilterFactor(context)).resolves.toBe(1);
      await expect(source.getSelectorShape(context)).resolves.toBe('shape');
      expect(source.queryQuads(ask, context)).toBe('quads');
      await expect(source.queryBoolean(ask, context)).resolves.toBe(true);
      await expect(source.queryVoid(ask, context)).resolves.toBeUndefined();
      expect(source.toString()).toBe('QuerySourceExtractLinks(inner)');
    });

    it('passes on the bindings of an operation without triple patterns unchanged', () => {
      const tracker = manager();
      const source = new QuerySourceExtractLinks(inner, URL, linkMediator([]), tracker, DF);
      const stream = source.queryBindings(AF.createNop(), context);
      expect(stream).toBe((<any> inner.queryBindings).mock.results[0].value);
      expect(tracker.addDereferencingDerivedResource).not.toHaveBeenCalled();
    });
  });

  describe('extracting links', () => {
    const pattern = AF.createPattern(
      DF.variable('review'),
      DF.namedNode('http://ex.org/reviewer'),
      DF.variable('reviewer'),
    );

    it('passes the bindings on and hands the links of the matched triples to the manager', async() => {
      const seen: RDF.Quad[] = [];
      const mediator = linkMediator(seen);
      const tracker = manager();
      const source = new QuerySourceExtractLinks(inner, URL, mediator, tracker, DF);
      const stream = source.queryBindings(AF.createProject(pattern, []), context);

      await expect(new Promise(resolve => stream.getProperty('metadata', resolve)))
        .resolves.toEqual({ cardinality: 2 });
      await expect(stream.toArray()).resolves.toEqual(rows);
      await flush();

      // Only the first row binds every variable of the pattern
      expect(seen).toEqual([
        DF.quad(DF.namedNode('http://ex.org/r1'), DF.namedNode('http://ex.org/reviewer'), DF.namedNode('http://ex.org/p1')),
      ]);
      expect(mediator.mediate).toHaveBeenCalledWith(expect.objectContaining({ url: URL, requestTime: 0, context }));
      expect(tracker.addLinks).toHaveBeenCalledWith({ url: URL }, [{ url: 'http://ex.org/r1' }]);
      const controller = tracker.addDereferencingDerivedResource.mock.calls[0][0];
      expect(tracker.completeDereferencingDerivedResource).toHaveBeenCalledWith(controller);
    });

    it('rebuilds quads in the graph the pattern names, or the default graph', () => {
      const source = new QuerySourceExtractLinks(inner, URL, linkMediator([]), manager(), DF);
      const graphPattern = AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o'), DF.variable('g'));
      const bindings = BF.bindings([
        [ DF.variable('s'), DF.namedNode('s') ],
        [ DF.variable('p'), DF.namedNode('p') ],
        [ DF.variable('o'), DF.literal('o') ],
      ]);
      expect(source.matchedQuads([ graphPattern ], bindings)).toEqual([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.literal('o'), DF.defaultGraph()),
      ]);
      expect(source.matchedQuads([ graphPattern ], bindings.set(DF.variable('g'), DF.namedNode('g')))).toEqual([
        DF.quad(DF.namedNode('s'), DF.namedNode('p'), DF.literal('o'), DF.namedNode('g')),
      ]);
    });

    it('ends extraction when the bindings are destroyed before they end', async() => {
      const open = new BufferedIterator<RDF.Bindings>();
      (<any> inner.queryBindings).mockReturnValue(open);
      const seen: RDF.Quad[] = [];
      const tracker = manager();
      const source = new QuerySourceExtractLinks(inner, URL, linkMediator(seen), tracker, DF);
      const stream = source.queryBindings(pattern, context);
      stream.destroy();
      await flush();
      expect(tracker.addLinks).toHaveBeenCalledWith({ url: URL }, []);
      expect(tracker.completeDereferencingDerivedResource).toHaveBeenCalledTimes(1);
    });

    it('only removes its work when traversal has not started', async() => {
      const tracker = manager(false);
      const source = new QuerySourceExtractLinks(inner, URL, linkMediator([]), tracker, DF);
      await source.queryBindings(pattern, context).toArray();
      await flush();
      expect(tracker.completeDereferencingDerivedResource).not.toHaveBeenCalled();
      expect(tracker.removeDereferencingDerivedResource)
        .toHaveBeenCalledWith(tracker.addDereferencingDerivedResource.mock.calls[0][0]);
    });

    it('does not hand over links once traversal has aborted the work', async() => {
      const tracker = manager();
      tracker.addDereferencingDerivedResource.mockImplementation((controller: AbortController) => controller.abort());
      const source = new QuerySourceExtractLinks(inner, URL, linkMediator([]), tracker, DF);
      await source.queryBindings(pattern, context).toArray();
      await flush();
      expect(tracker.addLinks).not.toHaveBeenCalled();
      expect(tracker.completeDereferencingDerivedResource).toHaveBeenCalledTimes(1);
    });

    it('does not fail when links cannot be extracted', async() => {
      const tracker = manager();
      const mediator = { mediate: jest.fn(async() => {
        throw new Error('extraction failed');
      }) };
      const source = new QuerySourceExtractLinks(inner, URL, <any> mediator, tracker, DF);
      await expect(source.queryBindings(pattern, context).toArray()).resolves.toEqual(rows);
      await flush();
      expect(tracker.addLinks).not.toHaveBeenCalled();
      expect(tracker.completeDereferencingDerivedResource).toHaveBeenCalledTimes(1);
    });
  });

  describe('patterns', () => {
    it('collects the triple patterns of an operation', () => {
      const first = AF.createPattern(DF.variable('a'), DF.namedNode('p'), DF.variable('b'));
      const second = AF.createPattern(DF.variable('b'), DF.namedNode('q'), DF.variable('c'));
      expect(QuerySourceExtractLinks.patterns(AF.createJoin([ first, second ]))).toEqual([ first, second ]);
    });
  });
});
