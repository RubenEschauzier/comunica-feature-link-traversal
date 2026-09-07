import { Bus, ActionContext } from '@comunica/core';
import { DataFactory } from 'rdf-data-factory';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { termToString } from 'rdf-string';

// @rdfjs/to-ntriples is ESM only, so it needs to be stubbed out for jest
jest.mock('@rdfjs/to-ntriples', () => ({
  __esModule: true,
  default: (term: any) => termToString(term),
}));

import { ActorDerivedResourceIdentifyLinearQuery } from '../lib/ActorDerivedResourceIdentifyLinearQuery';
import { QuerySourceParameterizedLinearQuery } from '../lib/QuerySourceParameterizedLinearQuery';
import { findLinearOrder } from '../lib/LinearShape';
import '@comunica/utils-jest';

const DF = new DataFactory();
const AF = new AlgebraFactory(DF);

describe('ActorDerivedResourceIdentifyLinearQuery', () => {
  let bus: any;
  let mediatorDereference: any;
  let mediatorQueryParse: any;
  let actor: ActorDerivedResourceIdentifyLinearQuery;

  const VAR_S = DF.variable('s');
  const VAR_O1 = DF.variable('o1');
  const VAR_O2 = DF.variable('o2');
  const VAR_PARAM_P1 = DF.variable('__param_p1');
  const VAR_PARAM_P2 = DF.variable('__param_p2');

  // ?s $p1$ ?o1 . ?o1 $p2$ ?o2
  const CHAIN_1 = AF.createPattern(VAR_S, VAR_PARAM_P1, VAR_O1);
  const CHAIN_2 = AF.createPattern(VAR_O1, VAR_PARAM_P2, VAR_O2);

  const template = 'linear/{p1}/{p2}';
  const baseUrl = 'http://example.org/pod/';
  const filter = 'SELECT ?s ?o1 ?o2 WHERE { ?s $p1$ ?o1 . ?o1 $p2$ ?o2 }';

  function createAction(overrides: Record<string, any> = {}) {
    return {
      context: new ActionContext(),
      derivedResourceUnidentified: {
        baseUrl,
        template,
        filter,
        selectors: [],
        ...overrides,
      },
    } as any;
  }

  function mockParse(patterns: any[], variables = [ VAR_S, VAR_O1, VAR_O2 ]) {
    mediatorQueryParse.mediate.mockResolvedValue({
      operation: AF.createProject(AF.createBgp(patterns), variables),
    });
  }

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    mediatorDereference = { mediate: jest.fn() };
    mediatorQueryParse = { mediate: jest.fn() };
    actor = new ActorDerivedResourceIdentifyLinearQuery({
      name: 'actor',
      bus,
      mediatorDereference,
      mediatorQueryParse,
    });
  });

  describe('findLinearOrder', () => {
    it('should return undefined for less than two patterns', () => {
      expect(findLinearOrder([])).toBeUndefined();
      expect(findLinearOrder([ CHAIN_1 ])).toBeUndefined();
    });

    it('should return undefined when a subject or object is not a variable', () => {
      const constantSubject = AF.createPattern(DF.namedNode('http://example.org/s'), VAR_PARAM_P1, VAR_O1);
      expect(findLinearOrder([ constantSubject, CHAIN_2 ])).toBeUndefined();

      const constantObject = AF.createPattern(VAR_O1, VAR_PARAM_P2, DF.literal('constant'));
      expect(findLinearOrder([ CHAIN_1, constantObject ])).toBeUndefined();
    });

    it('should return undefined for a star shape', () => {
      const star2 = AF.createPattern(VAR_S, VAR_PARAM_P2, VAR_O2);
      expect(findLinearOrder([ CHAIN_1, star2 ])).toBeUndefined();
    });

    it('should return undefined when a variable is reused as object', () => {
      const reusedObject = AF.createPattern(VAR_O1, VAR_PARAM_P2, VAR_O1);
      expect(findLinearOrder([ CHAIN_1, reusedObject ])).toBeUndefined();
    });

    it('should return undefined for a cycle', () => {
      const cycle1 = AF.createPattern(VAR_S, VAR_PARAM_P1, VAR_O1);
      const cycle2 = AF.createPattern(VAR_O1, VAR_PARAM_P2, VAR_S);
      expect(findLinearOrder([ cycle1, cycle2 ])).toBeUndefined();
    });

    it('should return undefined when only part of the patterns is on the path', () => {
      const disconnected1 = AF.createPattern(DF.variable('a'), VAR_PARAM_P1, DF.variable('b'));
      const disconnected2 = AF.createPattern(DF.variable('c'), VAR_PARAM_P2, DF.variable('d'));
      const disconnected3 = AF.createPattern(DF.variable('d'), DF.variable('__param_p3'), DF.variable('c'));
      expect(findLinearOrder([ disconnected1, disconnected2, disconnected3 ])).toBeUndefined();
    });

    it('should order the patterns of a path that is given out of order', () => {
      expect(findLinearOrder([ CHAIN_2, CHAIN_1 ])).toEqual([ CHAIN_1, CHAIN_2 ]);
    });

    it('should keep the patterns of a path that is already in order', () => {
      expect(findLinearOrder([ CHAIN_1, CHAIN_2 ])).toEqual([ CHAIN_1, CHAIN_2 ]);
    });
  });

  describe('selectsLinearVariables', () => {
    it('should return false if the start of the path is not projected', () => {
      expect(actor.selectsLinearVariables([ CHAIN_1, CHAIN_2 ], [ VAR_O1, VAR_O2 ])).toBe(false);
    });

    it('should return false if an object of the path is not projected', () => {
      expect(actor.selectsLinearVariables([ CHAIN_1, CHAIN_2 ], [ VAR_S, VAR_O1 ])).toBe(false);
    });

    it('should return true if all variables of the path are projected', () => {
      expect(actor.selectsLinearVariables([ CHAIN_1, CHAIN_2 ], [ VAR_S, VAR_O1, VAR_O2 ])).toBe(true);
    });
  });

  describe('test', () => {
    it('should fail without parameters in the template', async () => {
      await expect(actor.test(createAction({ template: 'linear' })))
        .resolves.toFailTest(`actor requires parameters in template of derived resource`);
    });

    it('should fail when parsing the query fails', async () => {
      mediatorQueryParse.mediate.mockRejectedValue(new Error('bad query'));
      await expect(actor.test(createAction()))
        .resolves.toFailTest(`actor parsing query failed with: bad query`);
    });

    it('should fail for a query that is not a select query', async () => {
      mediatorQueryParse.mediate.mockResolvedValue({
        operation: AF.createConstruct(AF.createBgp([ CHAIN_1, CHAIN_2 ]), [ CHAIN_1 ]),
      });
      await expect(actor.test(createAction()))
        .resolves.toFailTest(`actor only works with select templates`);
    });

    it('should fail for a select query without a plain BGP', async () => {
      mediatorQueryParse.mediate.mockResolvedValue({
        operation: AF.createProject(
          AF.createUnion([ AF.createBgp([ CHAIN_1 ]), AF.createBgp([ CHAIN_2 ]) ]),
          [ VAR_S ],
        ),
      });
      await expect(actor.test(createAction()))
        .resolves.toFailTest(`actor requires a WHERE clause with only a BGP`);
    });

    it('should fail for a query that is not path-shaped', async () => {
      mockParse([ CHAIN_1, AF.createPattern(VAR_S, VAR_PARAM_P2, VAR_O2) ]);
      await expect(actor.test(createAction()))
        .resolves.toFailTest(`actor requires a path-shaped query chaining distinct variables`);
    });

    it('should fail when not all path variables are projected', async () => {
      mockParse([ CHAIN_1, CHAIN_2 ], [ VAR_S, VAR_O1 ]);
      await expect(actor.test(createAction()))
        .resolves.toFailTest(`actor requires the select clause to project all path variables`);
    });

    it('should fail when a predicate of the path is not a parameter', async () => {
      mockParse([ CHAIN_1, AF.createPattern(VAR_O1, DF.namedNode('http://example.org/p2'), VAR_O2) ]);
      await expect(actor.test(createAction()))
        .resolves.toFailTest(`actor requires exclusively predicate parameters without repeats`);
    });

    it('should fail when the template has fewer parameters than the path has patterns', async () => {
      mockParse([ CHAIN_1, CHAIN_2 ]);
      await expect(actor.test(createAction({ template: 'linear/{p1}' })))
        .resolves.toFailTest(`actor requires exclusively predicate parameters without repeats`);
    });

    it('should pass for a path-shaped query with parameterized predicates', async () => {
      mockParse([ CHAIN_1, CHAIN_2 ]);
      await expect(actor.test(createAction())).resolves.toPassTest(true);
    });

    it('should pass for a path-shaped query given out of order', async () => {
      mockParse([ CHAIN_2, CHAIN_1 ]);
      await expect(actor.test(createAction())).resolves.toPassTest(true);
    });
  });

  describe('run', () => {
    it('should identify the derived resource with a parameterized linear query source', async () => {
      const sideData = {
        parameters: new Set([ 'p1', 'p2' ]),
        operation: AF.createProject(AF.createBgp([ CHAIN_1, CHAIN_2 ]), [ VAR_S, VAR_O1, VAR_O2 ]),
      };
      const { derivedResourceIdentified } = await actor.run(createAction(), <any> sideData);

      // Resolving the template against the base URL percent-encodes the parameter braces
      expect(derivedResourceIdentified.iri).toBe(`${baseUrl}linear/%7Bp1%7D/%7Bp2%7D`);
      expect(derivedResourceIdentified.querySource).toBeInstanceOf(QuerySourceParameterizedLinearQuery);
      expect(derivedResourceIdentified.derivedResourceSelectorShape).toEqual({
        type: 'operation',
        operation: {
          operationType: 'pattern',
          pattern: AF.createBgp([
            AF.createPattern(VAR_S, DF.variable('p1'), VAR_O1),
            AF.createPattern(VAR_O1, DF.variable('p2'), VAR_O2),
          ]),
        },
        variablesOptional: [ DF.variable('p1'), DF.variable('p2') ],
      });
      expect(derivedResourceIdentified.resourceCoefficients).toEqual({
        selectivity: 1,
        requests: 1,
        compute: 5,
      });
    });
  });
});
