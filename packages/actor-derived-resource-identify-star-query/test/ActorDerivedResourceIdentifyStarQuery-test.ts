import { Bus, ActionContext } from '@comunica/core';
import { DataFactory } from 'rdf-data-factory';
import { AlgebraFactory, Algebra } from '@comunica/utils-algebra';
import { termToString } from 'rdf-string';
jest.mock('@rdfjs/to-ntriples', () => ({
  __esModule: true,
  default: (term: any) => termToString(term),
}));
import { ActorDerivedResourceIdentifyStarQuery } from '../lib/ActorDerivedResourceIdentifyStarQuery';
import { QuerySourceParameterizedStarQuery } from '../lib/QuerySourceParameterizedStarQuery';
import '@comunica/utils-jest';

const DF = new DataFactory();
const AF = new AlgebraFactory(DF);

describe('ActorDerivedResourceIdentifyStarQuery', () => {
  let bus: any;
  let mediatorDereference: any;
  let mediatorQuerySourceDereferenceLink: any;
  let mediatorQueryParse: any;
  let actor: ActorDerivedResourceIdentifyStarQuery;

  const VAR_S = DF.variable('s');
  const VAR_O1 = DF.variable('o1');
  const VAR_O2 = DF.variable('o2');
  const VAR_PARAM_NAME = DF.variable('__param_name');
  const VAR_PARAM_AGE = DF.variable('__param_age');

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    mediatorDereference = { mediate: jest.fn() };
    mediatorQuerySourceDereferenceLink = { mediate: jest.fn() };
    mediatorQueryParse = { mediate: jest.fn() };
    actor = new ActorDerivedResourceIdentifyStarQuery({
      name: 'actor',
      bus,
      mediatorDereference,
      mediatorQuerySourceDereferenceLink,
      mediatorQueryParse,
    });
  });

  describe('helper methods', () => {
    describe('extractTemplateParams', () => {
      it('should extract single parameter', () => {
        expect(actor.extractTemplateParams('http://example.org/resource/{param}')).toEqual(new Set(['param']));
      });

      it('should extract multiple parameters', () => {
        expect(actor.extractTemplateParams('http://example.org/{param1}/item/{param2}')).toEqual(
          new Set(['param1', 'param2']),
        );
      });

      it('should return empty set when no template parameters exist', () => {
        expect(actor.extractTemplateParams('http://example.org/resource/static')).toEqual(new Set());
      });
    });

    describe('isStarShaped', () => {
      it('should return false for empty patterns', () => {
        expect(actor.isStarShaped(AF.createBgp([]))).toBe(false);
      });

      it('should return false for single pattern', () => {
        const pattern = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
        expect(actor.isStarShaped(AF.createBgp([pattern]))).toBe(false);
      });

      it('should return false when root subject is not a Variable', () => {
        const namedNodeS = DF.namedNode('http://example.org/s');
        const p1 = AF.createPattern(namedNodeS, VAR_PARAM_NAME, VAR_O1);
        const p2 = AF.createPattern(namedNodeS, VAR_PARAM_AGE, VAR_O2);
        expect(actor.isStarShaped(AF.createBgp([p1, p2]))).toBe(false);
      });

      it('should return false when patterns have different subjects', () => {
        const p1 = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
        const p2 = AF.createPattern(DF.variable('otherS'), VAR_PARAM_AGE, VAR_O2);
        expect(actor.isStarShaped(AF.createBgp([p1, p2]))).toBe(false);
      });

      it('should return false when an object is not a Variable', () => {
        const p1 = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
        const p2 = AF.createPattern(VAR_S, VAR_PARAM_AGE, DF.literal('constant'));
        expect(actor.isStarShaped(AF.createBgp([p1, p2]))).toBe(false);
      });

      it('should return false when an object equals root subject', () => {
        const p1 = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
        const p2 = AF.createPattern(VAR_S, VAR_PARAM_AGE, VAR_S);
        expect(actor.isStarShaped(AF.createBgp([p1, p2]))).toBe(false);
      });

      it('should return false when objects across patterns are not distinct', () => {
        const p1 = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
        const p2 = AF.createPattern(VAR_S, VAR_PARAM_AGE, VAR_O1);
        expect(actor.isStarShaped(AF.createBgp([p1, p2]))).toBe(false);
      });

      it('should return true for valid star-shaped patterns', () => {
        const p1 = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
        const p2 = AF.createPattern(VAR_S, VAR_PARAM_AGE, VAR_O2);
        expect(actor.isStarShaped(AF.createBgp([p1, p2]))).toBe(true);
      });
    });

    describe('selectsStarVariables', () => {
      const p1 = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
      const p2 = AF.createPattern(VAR_S, VAR_PARAM_AGE, VAR_O2);
      const bgp = AF.createBgp([p1, p2]);

      it('should return false if subject variable is missing in projected variables', () => {
        expect(actor.selectsStarVariables(bgp, [VAR_O1, VAR_O2])).toBe(false);
      });

      it('should return false if an object variable is missing in projected variables', () => {
        expect(actor.selectsStarVariables(bgp, [VAR_S, VAR_O1])).toBe(false);
      });

      it('should return true when subject and all object variables are projected', () => {
        expect(actor.selectsStarVariables(bgp, [VAR_S, VAR_O1, VAR_O2])).toBe(true);
      });

      it('should return true when extra variables are projected (e.g. SELECT *)', () => {
        expect(actor.selectsStarVariables(bgp, [VAR_S, VAR_O1, VAR_O2, DF.variable('extra')])).toBe(true);
      });
    });

    describe('constructsEqualStar', () => {
      const p1 = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
      const p2 = AF.createPattern(VAR_S, VAR_PARAM_AGE, VAR_O2);
      const p3 = AF.createPattern(VAR_S, DF.variable('__param_email'), DF.variable('o3'));

      it('should return false if lengths differ', () => {
        expect(actor.constructsEqualStar(AF.createBgp([p1, p2]), [p1])).toBe(false);
      });

      it('should return false if patterns do not match', () => {
        expect(actor.constructsEqualStar(AF.createBgp([p1, p2]), [p1, p3])).toBe(false);
      });

      it('should return true if patterns match in same order', () => {
        expect(actor.constructsEqualStar(AF.createBgp([p1, p2]), [p1, p2])).toBe(true);
      });

      it('should return true if patterns match in different order', () => {
        expect(actor.constructsEqualStar(AF.createBgp([p1, p2]), [p2, p1])).toBe(true);
      });
    });

    describe('allPredicatesParameters', () => {
      it('should return true when all predicates are parameters', () => {
        const p1 = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
        const p2 = AF.createPattern(VAR_S, VAR_PARAM_AGE, VAR_O2);
        expect(actor.allPredicatesParameters([p1, p2], new Set(['name', 'age']))).toBe(true);
      });

      it('should return false when a predicate is not in parameters', () => {
        const p1 = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
        const p2 = AF.createPattern(VAR_S, DF.namedNode('http://example.org/other'), VAR_O2);
        expect(actor.allPredicatesParameters([p1, p2], new Set(['name']))).toBe(false);
      });
    });

    describe('normalizeQuery', () => {
      it('should replace $param$ occurrences with ?__param_<param>', () => {
        const query = 'SELECT ?s ?o1 ?o2 WHERE { ?s $name$ ?o1 . ?s $age$ ?o2 }';
        const params = new Set(['name', 'age']);
        const { normalized, newVariables } = actor.normalizeQuery(query, params);

        expect(normalized).toBe(
          'SELECT ?s ?o1 ?o2 WHERE { ?s ?__param_name ?o1 . ?s ?__param_age ?o2 }',
        );
        expect(newVariables).toEqual(new Set(['?__param_name', '?__param_age']));
      });
    });
  });

  describe('test', () => {
    let action: any;
    let pattern1: Algebra.Pattern;
    let pattern2: Algebra.Pattern;
    let validBgp: Algebra.Bgp;
    let validProject: Algebra.Project;

    beforeEach(() => {
      action = {
        context: new ActionContext(),
        derivedResourceUnidentified: {
          baseUrl: 'http://example.org/',
          template: 'http://example.org/data/{name}/{age}',
          selectors: ['*'],
          filter: 'SELECT ?s ?o1 ?o2 WHERE { ?s $name$ ?o1 . ?s $age$ ?o2 }',
        },
      };

      pattern1 = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
      pattern2 = AF.createPattern(VAR_S, VAR_PARAM_AGE, VAR_O2);
      validBgp = AF.createBgp([pattern1, pattern2]);
      validProject = AF.createProject(validBgp, [VAR_S, VAR_O1, VAR_O2]);
    });

    it('should fail when template has no parameters', async () => {
      action.derivedResourceUnidentified.template = 'http://example.org/data/static';
      await expect(actor.test(action)).resolves.toFailTest(
        'actor requires parameters in template of derived resource',
      );
    });

    it('should fail when query parsing fails', async () => {
      mediatorQueryParse.mediate.mockRejectedValue(new Error('Parse syntax error'));
      await expect(actor.test(action)).resolves.toFailTest(
        'actor parsing query failed with: Parse syntax error',
      );
    });

    it('should fail when parsed operation is not PROJECT', async () => {
      mediatorQueryParse.mediate.mockResolvedValue({
        operation: AF.createConstruct(validBgp, [pattern1, pattern2]),
      });
      await expect(actor.test(action)).resolves.toFailTest(
        'actor only works with select templates',
      );
    });

    it('should fail when WHERE clause is not a BGP', async () => {
      mediatorQueryParse.mediate.mockResolvedValue({
        operation: AF.createProject(
          AF.createUnion([validBgp, AF.createBgp([])]),
          [VAR_S, VAR_O1, VAR_O2],
        ),
      });
      await expect(actor.test(action)).resolves.toFailTest(
        'actor requires a WHERE clause with only a BGP',
      );
    });

    it('should fail when WHERE clause BGP is not star shaped', async () => {
      const invalidBgp = AF.createBgp([pattern1]); // only 1 pattern
      mediatorQueryParse.mediate.mockResolvedValue({
        operation: AF.createProject(invalidBgp, [VAR_S, VAR_O1]),
      });
      await expect(actor.test(action)).resolves.toFailTest(
        'actor requires a star-shaped query with all objects different and variable subject',
      );
    });

    it('should fail when SELECT clause does not project all star variables', async () => {
      // Omit ?o2 from projected variables
      mediatorQueryParse.mediate.mockResolvedValue({
        operation: AF.createProject(validBgp, [VAR_S, VAR_O1]),
      });
      await expect(actor.test(action)).resolves.toFailTest(
        'actor requires the select clause to project all star variables',
      );
    });

    it('should fail when parameters size < patterns length or predicates repeat', async () => {
      // Duplicate predicate in patterns
      const pDup = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O2);
      const dupBgp = AF.createBgp([pattern1, pDup]);
      mediatorQueryParse.mediate.mockResolvedValue({
        operation: AF.createProject(dupBgp, [VAR_S, VAR_O1, VAR_O2]),
      });
      await expect(actor.test(action)).resolves.toFailTest(
        'actor requires exclusively predicate parameters without repeats',
      );
    });

    it('should pass and return sideData for valid star select query', async () => {
      mediatorQueryParse.mediate.mockResolvedValue({
        operation: validProject,
      });

      const result = await actor.test(action);
      expect(result).toMatchObject({
        sideData: {
          parameters: new Set(['name', 'age']),
          operation: validProject,
        },
      });
    });
  });

  describe('run', () => {
    it('should construct query source and return identified derived resource', async () => {
      const pattern1 = AF.createPattern(VAR_S, VAR_PARAM_NAME, VAR_O1);
      const pattern2 = AF.createPattern(VAR_S, VAR_PARAM_AGE, VAR_O2);
      const bgp = AF.createBgp([pattern1, pattern2]);
      const project = AF.createProject(bgp, [VAR_S, VAR_O1, VAR_O2]);

      const action: any = {
        context: new ActionContext(),
        derivedResourceUnidentified: {
          baseUrl: 'http://example.org/',
          template: 'data/{name}/{age}',
          selectors: ['*'],
          filter: 'SELECT ?s ?o1 ?o2 WHERE { ?s $name$ ?o1 . ?s $age$ ?o2 }',
        },
      };

      const sideData: any = {
        parameters: new Set(['name', 'age']),
        operation: project,
      };

      const output = await actor.run(action, sideData);
      expect(output.derivedResourceIdentified).toBeDefined();
      expect(output.derivedResourceIdentified.iri).toBe('http://example.org/data/%7Bname%7D/%7Bage%7D');
      expect(output.derivedResourceIdentified.querySource).toBeInstanceOf(QuerySourceParameterizedStarQuery);
      expect(output.derivedResourceIdentified.resourceCoefficients).toEqual({
        selectivity: 1,
        requests: 1,
        compute: 5,
      });
      expect(output.derivedResourceIdentified.derivedResourceSelectorShape).toEqual({
        type: 'operation',
        operation: {
          operationType: 'pattern',
          pattern: AF.createBgp([
            AF.createPattern(VAR_S, DF.variable('name'), VAR_O1),
            AF.createPattern(VAR_S, DF.variable('age'), VAR_O2),
          ]),
        },
        variablesOptional: [DF.variable('name'), DF.variable('age')],
      });
    });
  });
});
