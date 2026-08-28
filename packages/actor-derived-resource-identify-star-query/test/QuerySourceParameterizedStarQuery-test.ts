import { ActionContext } from '@comunica/core';
import { DataFactory } from 'rdf-data-factory';
import { AlgebraFactory, Algebra } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';
import { Readable } from 'stream';

jest.mock('@rdfjs/to-ntriples', () => ({
  __esModule: true,
  default: (term: any) => termToString(term),
}));

import { QuerySourceParameterizedStarQuery } from '../lib/QuerySourceParameterizedStarQuery';
import '@comunica/utils-jest';

const DF = new DataFactory();
const AF = new AlgebraFactory(DF);

function createSparqlJsonResponse(bindings: Record<string, RDF.Term>[]) {
  const json = JSON.stringify({
    head: { vars: Object.keys(bindings[0] || {}) },
    results: {
      bindings: bindings.map(row => {
        const rowObj: any = {};
        for (const [k, v] of Object.entries(row)) {
          if (v.termType === 'NamedNode') {
            rowObj[k] = { type: 'uri', value: v.value };
          } else if (v.termType === 'Literal') {
            rowObj[k] = { type: 'literal', value: v.value };
          } else {
            rowObj[k] = { type: 'bnode', value: v.value };
          }
        }
        return rowObj;
      }),
    },
  });
  return new Readable({
    read() {
      this.push(json);
      this.push(null);
    },
  });
}

describe('QuerySourceParameterizedStarQuery', () => {
  let mediatorDereference: any;
  let querySource: QuerySourceParameterizedStarQuery;
  let projectOperation: Algebra.Project;
  let parameters: Set<string>;
  const template = 'http://example.org/data/{p1}/{p2}';

  const VAR_S = DF.variable('s');
  const VAR_O1 = DF.variable('o1');
  const VAR_O2 = DF.variable('o2');
  const VAR_PARAM_P1 = DF.variable('__param_p1');
  const VAR_PARAM_P2 = DF.variable('__param_p2');

  const EX_P1 = DF.namedNode('http://example.org/p1');
  const EX_P2 = DF.namedNode('http://example.org/p2');

  beforeEach(() => {
    mediatorDereference = {
      mediate: jest.fn(),
    };

    parameters = new Set(['p1', 'p2']);
    const pattern1 = AF.createPattern(VAR_S, VAR_PARAM_P1, VAR_O1);
    const pattern2 = AF.createPattern(VAR_S, VAR_PARAM_P2, VAR_O2);
    const bgp = AF.createBgp([pattern1, pattern2]);
    projectOperation = AF.createProject(bgp, [VAR_S, VAR_O1, VAR_O2]);

    querySource = new QuerySourceParameterizedStarQuery(
      template,
      projectOperation,
      parameters,
      mediatorDereference,
      DF,
    );
  });

  describe('constructor', () => {
    it('should throw if input is not a BGP', () => {
      const invalidOperation: any = AF.createProject(
        AF.createUnion([AF.createBgp([]), AF.createBgp([])]),
        [VAR_S],
      );
      expect(
        () =>
          new QuerySourceParameterizedStarQuery(
            template,
            invalidOperation,
            parameters,
            mediatorDereference,
            DF,
          ),
      ).toThrow('Non-BGP passed to star query source');
    });

    it('should initialize referenceValue and template', () => {
      expect(querySource.referenceValue).toBe(template);
    });
  });

  describe('getSelectorShape', () => {
    it('should return the expected selector shape with cleaned variables', async () => {
      const shape = await querySource.getSelectorShape();
      expect(shape).toEqual({
        type: 'operation',
        operation: {
          operationType: 'pattern',
          pattern: AF.createBgp([
            AF.createPattern(VAR_S, DF.variable('p1'), VAR_O1),
            AF.createPattern(VAR_S, DF.variable('p2'), VAR_O2),
          ]),
        },
        variablesOptional: [DF.variable('p1'), DF.variable('p2')],
      });
    });
  });

  describe('getFilterFactor', () => {
    it('should return 0', async () => {
      expect(await querySource.getFilterFactor()).toBe(0);
    });
  });

  describe('toString', () => {
    it('should return QuerySourceParameterizedStarQuery with template', () => {
      expect(querySource.toString()).toBe(`QuerySourceParameterizedStarQuery(${template})`);
    });
  });

  describe('unsupported query operations', () => {
    const context = new ActionContext();

    it('should throw on queryQuads', () => {
      expect(() => querySource.queryQuads(<any>{}, context)).toThrow(
        'queryQuads is not implemented in QuerySourceParameterizedStarQuery',
      );
    });

    it('should throw on queryBoolean', () => {
      expect(() => querySource.queryBoolean(<any>{}, context)).toThrow(
        'queryBoolean is not implemented in QuerySourceParameterizedStarQuery',
      );
    });

    it('should throw on queryVoid', () => {
      expect(() => querySource.queryVoid(<any>{}, context)).toThrow(
        'queryVoid is not implemented in QuerySourceParameterizedStarQuery',
      );
    });
  });

  describe('queryBindings', () => {
    const context = new ActionContext();

    it('should throw if operation is not a BGP', () => {
      const nonBgp = AF.createUnion([AF.createBgp([])]);
      expect(() => querySource.queryBindings(nonBgp, context)).toThrow(
        'QuerySourceParameterizedStarQuery only accepts BGPs, got: union',
      );
    });

    it('should throw if operation cannot be answered by selector shape', () => {
      // 3 patterns instead of 2
      const unsupportedBgp = AF.createBgp([
        AF.createPattern(VAR_S, EX_P1, VAR_O1),
        AF.createPattern(VAR_S, EX_P2, VAR_O2),
        AF.createPattern(VAR_S, DF.namedNode('http://example.org/p3'), DF.variable('o3')),
      ]);
      expect(() => querySource.queryBindings(unsupportedBgp, context)).toThrow(
        'Attempted queryBindings using operation not supported by QuerySourceStarQuery',
      );
    });

    it('should dereference link with filled template and parse bindings from SPARQL JSON', async () => {
      const concreteBgp = AF.createBgp([
        AF.createPattern(VAR_S, EX_P1, VAR_O1),
        AF.createPattern(VAR_S, EX_P2, VAR_O2),
      ]);

      const expectedRow = {
        s: DF.namedNode('http://example.org/alice'),
        o1: DF.literal('Alice'),
        o2: DF.literal('30'),
      };
      mediatorDereference.mediate.mockResolvedValue({
        data: createSparqlJsonResponse([expectedRow]),
      });

      const bindingsStream = querySource.queryBindings(concreteBgp, context);
      const bindings = await bindingsStream.toArray();

      expect(bindings).toHaveLength(1);
      expect(bindings[0].get('s')).toEqual(expectedRow.s);
      expect(bindings[0].get('o1')).toEqual(expectedRow.o1);
      expect(bindings[0].get('o2')).toEqual(expectedRow.o2);

      expect(mediatorDereference.mediate).toHaveBeenCalledWith({
        url: `http://example.org/data/${encodeURIComponent(termToString(EX_P1))}/${encodeURIComponent(termToString(EX_P2))}`,
        method: 'GET',
        headers: expect.any(Headers),
        context,
      });
      const headersArg = mediatorDereference.mediate.mock.calls[0][0].headers;
      expect(headersArg.get('Accept')).toBe('application/sparql-results+json');
    });

    it('should dereference link mapping internal variable names', async () => {
      const internalVarBgp = AF.createBgp([
        AF.createPattern(VAR_S, DF.variable('__comunica:internal1'), VAR_O1),
        AF.createPattern(VAR_S, DF.variable('userVar'), VAR_O2),
      ]);

      mediatorDereference.mediate.mockResolvedValue({
        data: createSparqlJsonResponse([]),
      });

      const bindingsStream = querySource.queryBindings(internalVarBgp, context);
      await bindingsStream.toArray();

      // Internal variable __comunica:internal1 should be mapped to p1, userVar retained as ?userVar
      expect(mediatorDereference.mediate).toHaveBeenCalledWith({
        url: `http://example.org/data/%3Fp1/%3FuserVar`,
        method: 'GET',
        headers: expect.any(Headers),
        context,
      });
    });

    it('should destroy bindings stream proxy on dereference error', async () => {
      const concreteBgp = AF.createBgp([
        AF.createPattern(VAR_S, EX_P1, VAR_O1),
        AF.createPattern(VAR_S, EX_P2, VAR_O2),
      ]);

      mediatorDereference.mediate.mockRejectedValue(
        new Error('Network dereference error'),
      );

      const bindingsStream = querySource.queryBindings(concreteBgp, context);
      await expect(bindingsStream.toArray()).rejects.toThrow('Network dereference error');
    });
  });
});
