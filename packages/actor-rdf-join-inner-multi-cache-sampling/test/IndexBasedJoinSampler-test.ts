import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type { Pattern } from 'sparqlalgebrajs/lib/algebra';
import { types } from 'sparqlalgebrajs/lib/algebra';
import { IndexBasedJoinSampler } from '../lib/IndexBasedJoinSampler';
import type { CountFn, SampleFn } from '../lib/IndexBasedJoinSampler';

const DF = new DataFactory();

describe('IndexBasedJoinSampler', () => {
  let joinSampler: any;
  let spyOnMathRandom: any;

  beforeEach(() => {
    spyOnMathRandom = jest.spyOn(global.Math, 'random').mockReturnValue(0.4);
    joinSampler = new IndexBasedJoinSampler(0);
  });

  describe('sample array', () => {
    let array: number[];
    beforeEach(() => {
      array = [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ];
    });

    it('should correctly sample', () => {
      expect(joinSampler.sampleArray(array, 2)).toEqual([ 5, 1 ]);
    });

    it('should correctly sample on edge random number (0)', () => {
      spyOnMathRandom = jest.spyOn(global.Math, 'random').mockReturnValue(0);
      expect(joinSampler.sampleArray(array, 2)).toEqual([ 1, 2 ]);
    });

    it('should correctly sample on edge random number (.9999)', () => {
      spyOnMathRandom = jest.spyOn(global.Math, 'random').mockReturnValue(0.9999);
      expect(joinSampler.sampleArray(array, 2)).toEqual([ 10, 1 ]);
    });

    it('should sample uniformly', () => {
      // Function runs chi-squared test for uniform sampling distribution with alpha = .95
      spyOnMathRandom.mockRestore();
      const n_runs = 10_000;

      // eslint-disable-next-line
      const occurences: number[] = new Array(10).fill(0);
      for (let i = 0; i < n_runs; i++) {
        const sample = joinSampler.sampleArray(array, 4);
        for (const value of sample) {
          occurences[value]++;
        }
      }
      // Chi squared for uniform distribution
      const chiSquared = occurences.map(x => ((x - n_runs / 10) ^ 2) / (n_runs / 10))
        .reduce((acc, curr) => acc += curr, 0);
      // We have 10 categories (9 df), confidence of 0.05, chi squared 16.919
      expect(chiSquared > 16.9);
    });

    it('should return full array when n > array.length', () => {
      expect(joinSampler.sampleArray(array, 11)).toEqual([ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ]);
    });

    it('should return empty array if n = 0', () => {
      expect(joinSampler.sampleArray(array, 0)).toEqual([]);
    });
    it('should return empty array if n < 0', () => {
      expect(joinSampler.sampleArray(array, -1)).toEqual([]);
    });
  });

  describe('addQuadToBindings', () => {
    let quad: RDF.Quad;
    let bindings: Record<string, RDF.Term>;
    beforeEach(() => {
      quad = DF.quad(DF.namedNode('s'), DF.namedNode('o'), DF.namedNode('o'), DF.namedNode('g'));
      bindings = {};
    });
    it('should add variable value at subject', () => {
      const pattern: any = {
        subject: { termType: 'Variable', value: 'v0' },
        predicate: {
          termType: 'NamedNode',
          value: 'p0',
        },
        object: {
          termType: 'NamedNode',
          value: 'o0',
        },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      expect(joinSampler.addQuadToBindings(quad, bindings, pattern))
        .toEqual({ v0: DF.namedNode('s') });
    });
    it('should add variable value at subject', () => {
      const pattern: any = {
        subject: { termType: 'Variable', value: 'v0' },
        predicate: {
          termType: 'NamedNode',
          value: 'p0',
        },
        object: {
          termType: 'NamedNode',
          value: 'o0',
        },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      expect(joinSampler.addQuadToBindings(quad, bindings, pattern))
        .toEqual({ v0: DF.namedNode('s') });
    });
    it('should add variable value at predicate', () => {
      const pattern: any = {
        subject: {
          termType: 'NamedNode',
          value: 's0',
        },
        predicate: { termType: 'Variable', value: 'v0' },
        object: {
          termType: 'NamedNode',
          value: 'o0',
        },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      expect(joinSampler.addQuadToBindings(quad, bindings, pattern))
        .toEqual({ v0: DF.namedNode('p') });
    });
    it('should add variable value at object', () => {
      const pattern: any = {
        subject: {
          termType: 'NamedNode',
          value: 's0',
        },
        predicate: {
          termType: 'NamedNode',
          value: 'p0',
        },
        object: { termType: 'Variable', value: 'v0' },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      expect(joinSampler.addQuadToBindings(quad, bindings, pattern))
        .toEqual({ v0: DF.namedNode('o') });
    });
    it('should add variable value at graph', () => {
      const pattern: any = {
        subject: {
          termType: 'NamedNode',
          value: 's0',
        },
        predicate: {
          termType: 'NamedNode',
          value: 'p0',
        },
        object: {
          termType: 'NamedNode',
          value: 'o0',
        },
        graph: { termType: 'Variable', value: 'v0' },
      };
      expect(joinSampler.addQuadToBindings(quad, bindings, pattern))
        .toEqual({ v0: DF.namedNode('g') });
    });
  });

  describe('setSampleRelation', () => {
    let sample: Record<string, RDF.Term>;
    beforeEach(() => {
      sample = { v0: DF.namedNode('s'), v1: DF.namedNode('p'), v2: DF.namedNode('o'), v3: DF.namedNode('g'),
      };
    });
    it('should correctly fill subject when joinVariable is "s"', () => {
      const result = joinSampler.setSampleRelation(
        sample,
        undefined,
        DF.namedNode('p'),
        DF.namedNode('o'),
        DF.namedNode('g'),
        's',
        'v0',
      );
      expect(result).toEqual([ DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('o'), DF.namedNode('g') ]);
    });

    it('should correctly fill predicate when joinVariable is "p"', () => {
      const result = joinSampler.setSampleRelation(
        sample,
        DF.namedNode('s'),
        undefined,
        DF.namedNode('o'),
        DF.namedNode('g'),
        'p',
        'v1',
      );
      expect(result).toEqual([ DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('o'), DF.namedNode('g') ]);
    });

    it('should correctly fill object when joinVariable is "o"', () => {
      const result = joinSampler.setSampleRelation(
        sample,
        DF.namedNode('s'),
        DF.namedNode('p'),
        undefined,
        DF.namedNode('g'),
        'o',
        'v2',
      );
      expect(result).toEqual([ DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('o'), DF.namedNode('g') ]);
    });

    it('should correctly fill object when joinVariable is "g"', () => {
      const result = joinSampler.setSampleRelation(
        sample,
        DF.namedNode('s'),
        DF.namedNode('p'),
        DF.namedNode('o'),
        undefined,
        'g',
        'v3',
      );
      expect(result).toEqual([ DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('o'), DF.namedNode('g') ]);
    });

    it('should handle empty sample correctly', () => {
      const sample: Record<string, string> = {};
      expect(() => {
        joinSampler.setSampleRelation(sample, 'sub', 'pred', 'obj', 's', 'v0');
      }).toThrow(
        new Error('Sample should contain atleast a binding'),
      );
    });

    it('should handle null values for parameters', () => {
      const result = joinSampler.setSampleRelation(sample, undefined, undefined, undefined, undefined, 's', 'v0');
      expect(result).toEqual([ DF.namedNode('s'), undefined, undefined, undefined ]);
    });

    it('should throw an error for an invalid joinVariable', () => {
      expect(() => {
        joinSampler.setSampleRelation(sample, 'sub', 'pred', 'obj', 'invalid', 'v0');
      }).toThrow('Invalid join variable');
    });
  });

  describe('generateNewCombinations', () => {
    it('should throw an error when combination does not contain exactly 2 subarrays', () => {
      const combination = [[ 0 ], [ 1 ], [ 2 ]];
      const n = 3;
      expect(() => {
        joinSampler.generateNewCombinations(combination, n);
      }).toThrow('Combination should contain atleast two elements with length >= 1');
    });

    it('should throw an error when the first subarray in combination is empty', () => {
      const combination = [[], [ 1 ]];
      const n = 3;
      expect(() => {
        joinSampler.generateNewCombinations(combination, n);
      }).toThrow('Combination should contain atleast two elements with length >= 1');
    });

    it('should throw an error when the second subarray in combination is empty', () => {
      const combination = [[ 0 ], []];
      const n = 3;
      expect(() => {
        joinSampler.generateNewCombinations(combination, n);
      }).toThrow('Combination should contain atleast two elements with length >= 1');
    });

    // Basic Functionality Test
    it('should generate new combinations correctly for a valid input', () => {
      const combination = [[ 0 ], [ 1 ]];
      const n = 3;
      const result = joinSampler.generateNewCombinations(combination, n);
      expect(result).toEqual([
        [[ 0, 1 ], [ 2 ]],
      ]);
    });

    it('should generate new combinations correctly for a valid input', () => {
      const combination = [[ 0 ], [ 1 ]];
      const n = 4;
      const result = joinSampler.generateNewCombinations(combination, n);
      expect(result).toEqual([
        [[ 0, 1 ], [ 2 ]],
        [[ 0, 1 ], [ 3 ]],
      ]);
    });

    // Edge Case Test
    it('should return an empty array when n is 0', () => {
      const combination = [[ 0 ], [ 1 ]];
      const n = 0;
      expect(() => {
        joinSampler.generateNewCombinations(combination, n);
      }).toThrow('Received non-positive n');
    });

    it('should return an empty array when all numbers from 0 to n-1 are used', () => {
      const combination = [[ 0, 1 ], [ 2 ]];
      const n = 3;
      const result = joinSampler.generateNewCombinations(combination, n);
      expect(result).toEqual([]);
    });
  });

  describe('initializeSamplesEnumeration', () => {
    it('should initialize samples correctly for a non-empty resultSets', () => {
      const resultSets = [
        [[ 'a1', 'b1' ], [ 'c1', 'd1' ]],
        [[ 'a2', 'b2' ], [ 'c2', 'd2' ]],
      ];
      const cardinalities = [ 2, 5 ];
      const result = joinSampler.initializeSamplesEnumeration(resultSets, cardinalities);

      expect(result).toEqual(new Map(Object.entries({
        '[0]': { sample: [[ 'a1', 'b1' ], [ 'c1', 'd1' ]], estimatedCardinality: 2 },
        '[1]': { sample: [[ 'a2', 'b2' ], [ 'c2', 'd2' ]], estimatedCardinality: 5 },
      })));
    });

    it('should handle resultSets with a single resultSet', () => {
      const resultSets = [
        [[ 'a1', 'b1' ], [ 'c1', 'd1' ]],
      ];
      const cardinalities = [ 8 ];
      const result = joinSampler.initializeSamplesEnumeration(resultSets, cardinalities);

      expect(result).toEqual(new Map(Object.entries({
        '[0]': { sample: [[ 'a1', 'b1' ], [ 'c1', 'd1' ]], estimatedCardinality: 8 },
      })));
    });

    it('should handle resultSets with multiple empty arrays', () => {
      const resultSets = [
        [],
        [[ 'a2', 'b2' ], [ 'c2', 'd2' ]],
        [],
      ];
      const cardinalities = [ 0, 8, 0 ];

      const result = joinSampler.initializeSamplesEnumeration(resultSets, cardinalities);

      expect(result).toEqual(new Map(Object.entries({
        '[0]': { sample: [], estimatedCardinality: 0 },
        '[1]': { sample: [[ 'a2', 'b2' ], [ 'c2', 'd2' ]], estimatedCardinality: 8 },
        '[2]': { sample: [], estimatedCardinality: 0 },
      })));
    });

    it('should create unique keys even with similar content in resultSets', () => {
      const resultSets = [
        [[ 'a1', 'b1' ]],
        [[ 'a1', 'b1' ]],
      ];
      const cardinalities = [ 1, 1 ];

      const result = joinSampler.initializeSamplesEnumeration(resultSets, cardinalities);

      expect(result).toEqual(new Map(Object.entries({
        '[0]': { sample: [[ 'a1', 'b1' ]], estimatedCardinality: 1 },
        '[1]': { sample: [[ 'a1', 'b1' ]], estimatedCardinality: 1 },
      })));
    });
  });

  describe('joinCombinations', () => {
    it('should create all valid size 2 combinations (without order)', () => {
      const n = 4;
      expect(joinSampler.joinCombinations(n)).toEqual([
        [[ 0 ], [ 1 ]],
        [[ 0 ], [ 2 ]],
        [[ 0 ], [ 3 ]],
        [[ 1 ], [ 2 ]],
        [[ 1 ], [ 3 ]],
        [[ 2 ], [ 3 ]],
      ]);
    });
    it('should return empty when n = 0', () => {
      const n = 0;
      expect(joinSampler.joinCombinations(n)).toEqual([]);
    });
  });
  describe('determineJoinVariable', () => {
    let operation1: any;
    let operation2: any;
    let operation3: any;
    let operation4: any;
    let operation5: any;
    let operation6: any;
    let operation7: any;
    let operation8: any;

    beforeEach(() => {
      operation1 = {
        subject: { termType: 'Variable', value: 'v0' },
        predicate: {
          termType: 'NamedNode',
          value: 'p0',
        },
        object: { termType: 'Variable', value: 'v1' },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      operation2 = {
        subject: { termType: 'Variable', value: 'v0' },
        predicate: {
          termType: 'NamedNode',
          value: 'p1',
        },
        object: { termType: 'Variable', value: 'v2' },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      operation3 = {
        subject: { termType: 'Variable', value: 'v1' },
        predicate: {
          termType: 'NamedNode',
          value: 'p2',
        },
        object: { termType: 'Variable', value: 'v2' },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      operation4 = {
        subject: {
          termType: 'NamedNode',
          value: 's1',
        },
        predicate: { termType: 'Variable', value: 'p0' },
        object: { termType: 'Variable', value: 'v1' },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      operation5 = {
        subject: {
          termType: 'NamedNode',
          value: 's2',
        },
        predicate: { termType: 'Variable', value: 'p0' },
        object: {
          termType: 'NamedNode',
          value: 'o1',
        },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      operation6 = {
        subject: { termType: 'Variable', value: 'v2' },
        predicate: {
          termType: 'NamedNode',
          value: 'p2',
        },
        object: { termType: 'Variable', value: 'v3' },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      operation7 = {
        subject: { termType: 'Variable', value: 'v1' },
        predicate: {
          termType: 'NamedNode',
          value: 'p2',
        },
        object: { termType: 'Variable', value: 'v4' },
        graph: { termType: 'Variable', value: 'v0' },
      };
      operation8 = {
        subject: { termType: 'Variable', value: 'v2' },
        predicate: {
          termType: 'NamedNode',
          value: 'p2',
        },
        object: { termType: 'Variable', value: 'v3' },
        graph: { termType: 'Variable', value: 'v0' },
      };
    });

    it('should work with two triple patterns in star', () => {
      expect(joinSampler.determineJoinVariable([ operation1 ], operation2)).toEqual({ joinLocation: 's', joinVariable: 'v0' });
    });
    it('should work with two triple patterns in path (object)', () => {
      expect(joinSampler.determineJoinVariable([ operation1 ], operation3)).toEqual({ joinLocation: 's', joinVariable: 'v1' });
    });
    it('should work with two triple patterns in path (subject)', () => {
      expect(joinSampler.determineJoinVariable([ operation3 ], operation1)).toEqual({ joinLocation: 'o', joinVariable: 'v1' });
    });
    it('should work with two triple patterns with predicate join', () => {
      expect(joinSampler.determineJoinVariable([ operation4 ], operation5)).toEqual({ joinLocation: 'p', joinVariable: 'p0' });
    });
    it('should identify graph join variable', () => {
      expect(joinSampler.determineJoinVariable([ operation7 ], operation8)).toEqual({ joinLocation: 'g', joinVariable: 'v0' });
    });
    it('should correctly identify cartesian joins', () => {
      expect(joinSampler.determineJoinVariable([ operation1 ], operation6)).toEqual({ joinLocation: 'c', joinVariable: '' });
      expect(joinSampler.determineJoinVariable([ operation5 ], operation6)).toEqual({ joinLocation: 'c', joinVariable: '' });
    });
    it('should work with multiple triple patterns in intermediate result', () => {
      expect(joinSampler.determineJoinVariable([ operation1, operation2 ], operation6)).toEqual({ joinLocation: 's', joinVariable: 'v2' });
    });
  });

  describe('tpToSampleQuery', () => {
    it('should correctly identify subject variable', () => {
      const triplePattern = {
        subject: { termType: 'Variable', value: 'v0' },
        predicate: {
          termType: 'NamedNode',
          value: 'p0',
        },
        object: {
          termType: 'NamedNode',
          value: 'o0',
        },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      expect(joinSampler.tpToSampleQuery(triplePattern))
        .toEqual([
          undefined,
          { termType: 'NamedNode', value: 'p0' },
          { termType: 'NamedNode', value: 'o0' },
          { termType: 'NamedNode', value: 'g0' },
        ]);
    });
    it('should correctly identify predicate variable', () => {
      const triplePattern = {
        subject: {
          termType: 'NamedNode',
          value: 's0',
        },
        predicate: { termType: 'Variable', value: 'v0' },
        object: {
          termType: 'NamedNode',
          value: 'o0',
        },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      expect(joinSampler.tpToSampleQuery(triplePattern))
        .toEqual([
          { termType: 'NamedNode', value: 's0' },
          undefined,
          { termType: 'NamedNode', value: 'o0' },
          { termType: 'NamedNode', value: 'g0' },
        ]);
    });
    it('should correctly identify object variable', () => {
      const triplePattern = {
        subject: {
          termType: 'NamedNode',
          value: 's0',
        },
        predicate: {
          termType: 'NamedNode',
          value: 'p0',
        },
        object: { termType: 'Variable', value: 'v0' },
        graph: {
          termType: 'NamedNode',
          value: 'g0',
        },
      };
      expect(joinSampler.tpToSampleQuery(triplePattern))
        .toEqual([
          { termType: 'NamedNode', value: 's0' },
          { termType: 'NamedNode', value: 'p0' },
          undefined,
          { termType: 'NamedNode', value: 'g0' },
        ]);
    });
    it('should correctly identify graph variable', () => {
      const triplePattern = {
        subject: {
          termType: 'NamedNode',
          value: 's0',
        },
        predicate: {
          termType: 'NamedNode',
          value: 'p0',
        },
        object: {
          termType: 'NamedNode',
          value: 'o0',
        },
        graph: { termType: 'Variable', value: 'v0' },
      };
      expect(joinSampler.tpToSampleQuery(triplePattern))
        .toEqual([
          { termType: 'NamedNode', value: 's0' },
          { termType: 'NamedNode', value: 'p0' },
          { termType: 'NamedNode', value: 'o0' },
          undefined,
        ]);
    });
  });

  describe('sampleTriplePatternsQuery', () => {
    let sampleFn: SampleFn;
    let countFn: CountFn;

    beforeEach(() => {
      sampleFn = (indexes: number[], subject?: RDF.Term, predicate?: RDF.Term, object?: RDF.Term, graph?: RDF.Term) => {
        return [ DF.quad(DF.namedNode('s0'), DF.namedNode('p0'), DF.namedNode('o0'), DF.namedNode('g0')) ];
      };
      countFn = (subject?: RDF.Term, predicate?: RDF.Term, object?: RDF.Term, graph?: RDF.Term) => {
        return 7;
      };
    });
    it('should sample triple pattern with subject variable', async() => {
      const quad = DF.quad(DF.variable('v0'), DF.namedNode('p0'), DF.namedNode('o0'), DF.namedNode('g0'));
      const tps: Pattern[] = [{ ...quad, type: types.PATTERN }];
      await expect(
        joinSampler.sampleTriplePatternsQuery(1, tps, sampleFn, countFn),
      ).resolves.toEqual({
        sampledTriplePatterns: [[{ v0: DF.namedNode('s0') }]],
        cardinalities: [ 7 ],
      });
    });
    it('should sample triple pattern with predicate variable', async() => {
      const quad = DF.quad(DF.namedNode('s0'), DF.variable('v1'), DF.namedNode('o0'), DF.namedNode('g0'));
      const tps: Pattern[] = [{ ...quad, type: types.PATTERN }];
      await expect(
        joinSampler.sampleTriplePatternsQuery(1, tps, sampleFn, countFn),
      ).resolves.toEqual({
        sampledTriplePatterns: [[{ v1: DF.namedNode('p0') }]],
        cardinalities: [ 7 ],
      });
    });
    it('should sample triple pattern with object variable', async() => {
      const quad = DF.quad(DF.namedNode('s0'), DF.namedNode('p0'), DF.variable('v2'), DF.namedNode('g0'));
      const tps: Pattern[] = [{ ...quad, type: types.PATTERN }];
      await expect(
        joinSampler.sampleTriplePatternsQuery(1, tps, sampleFn, countFn),
      ).resolves.toEqual({
        sampledTriplePatterns: [[{ v2: DF.namedNode('o0') }]],
        cardinalities: [ 7 ],
      });
    });
    it('should sample triple pattern with subject variable', async() => {
      const quad = DF.quad(DF.namedNode('s0'), DF.namedNode('p0'), DF.namedNode('o0'), DF.variable('v3'));
      const tps: Pattern[] = [{ ...quad, type: types.PATTERN }];
      await expect(
        joinSampler.sampleTriplePatternsQuery(1, tps, sampleFn, countFn),
      ).resolves.toEqual({
        sampledTriplePatterns: [[{ v3: DF.namedNode('g0') }]],
        cardinalities: [ 7 ],
      });
    });
    it('should sample triple pattern with multiple variables', async() => {
      const quad = DF.quad(DF.variable('v0'), DF.namedNode('p0'), DF.namedNode('o0'), DF.variable('v3'));
      const tps: Pattern[] = [{ ...quad, type: types.PATTERN }];
      await expect(
        joinSampler.sampleTriplePatternsQuery(1, tps, sampleFn, countFn),
      ).resolves.toEqual({
        sampledTriplePatterns: [[{ v0: DF.namedNode('s0'), v3: DF.namedNode('g0') }]],
        cardinalities: [ 7 ],
      });
    });
  });
  // TODO
  describe('candidateCounts', () => {});

  describe('sampleJoin', () => {});

  describe('bottomUpEnumeration', () => {});

  // Describe('index-based functions', () => {
  //   let sampleArrayIndex: any;

  //   beforeEach(() => {
  //     // Sample data structure for arrayIndex
  //     sampleArrayIndex = {
  //       subjects: {
  //         s1: {
  //           p1: [ 'o1', 'o2' ],
  //           p2: [ 'o3' ],
  //         },
  //         s2: {
  //           p1: [ 'o4' ],
  //         },
  //         s3: {
  //           p1: [ 'o2' ],
  //         },
  //       },
  //       predicates: {
  //         p1: {
  //           o1: [ 's1' ],
  //           o2: [ 's3', 's1' ],
  //           o4: [ 's2' ],
  //         },
  //         p2: {
  //           o3: [ 's1' ],
  //         },
  //       },
  //       objects: {
  //         o1: {
  //           s1: [ 'p1' ],
  //         },
  //         o2: {
  //           s1: [ 'p1' ],
  //           s3: [ 'p1' ],
  //         },
  //         o3: {
  //           s1: [ 'p2' ],
  //         },
  //         o4: {
  //           s2: [ 'p1' ],
  //         },
  //       },
  //     };
  //   });
  //   describe('countCandidates', () => {
  //     let operation1: any;
  //     let operation2: any;

  //     beforeEach(() => {
  //       operation1 = {
  //         subject: { termType: 'Variable', value: 'v0' },
  //         predicate: {
  //           termType: 'NamedNode',
  //           value: 'p1',
  //         },
  //         object: { termType: 'Variable', value: 'v1' },
  //       };
  //       operation2 = {
  //         subject: { termType: 'Variable', value: 'v0' },
  //         predicate: {
  //           termType: 'NamedNode',
  //           value: 'p2',
  //         },
  //         object: { termType: 'Variable', value: 'v2' },
  //       };

  //       // Redefine arrayIndex to get more extensive result sets
  //       sampleArrayIndex = {
  //         subjects: {
  //           s1: {
  //             p1: [ 'o1', 'o2', 'o5' ],
  //             p2: [ 'o3' ],
  //           },
  //           s2: {
  //             p1: [ 'o4', 'o5' ],
  //             p2: [ 'o1', 'o4' ],
  //           },
  //           s3: {
  //             p1: [ 'o2', 'o4' ],
  //           },
  //         },
  //         predicates: {
  //           p1: {
  //             o1: [ 's1' ],
  //             o2: [ 's3', 's1' ],
  //             o4: [ 's2', 's3' ],
  //             o5: [ 's1', 's2' ],
  //           },
  //           p2: {
  //             o3: [ 's1' ],
  //             o1: [ 's2' ],
  //             o4: [ 's2' ],
  //           },
  //         },
  //         objects: {
  //           o1: {
  //             s1: [ 'p1' ],
  //             s2: [ 'p2' ],
  //           },
  //           o2: {
  //             s1: [ 'p1' ],
  //             s3: [ 'p1' ],
  //           },
  //           o3: {
  //             s1: [ 'p2' ],
  //           },
  //           o4: {
  //             s2: [ 'p1', 'p2' ],
  //             s3: [ 'p1' ],
  //           },
  //           o5: {
  //             s1: [ 'p1' ],
  //             s2: [ 'p2' ],
  //           },
  //         },
  //       };
  //     });

  //     it('should correctly count', () => {
  //       const samples: Record<string, string>[] = joinSampler.sampleIndex(
  //         null,
  //         'p2',
  //         null,
  //         Number.POSITIVE_INFINITY,
  //         sampleArrayIndex,
  //       ).map((x: string[]) => {
  //         return { v0: x[0], v2: x[2] };
  //       });
  //       expect(samples).toEqual(
  //         [{ v0: 's1', v2: 'o3' }, { v0: 's2', v2: 'o1' }, { v0: 's2', v2: 'o4' }],
  //       );

  //       expect(joinSampler.candidateCounts(samples, null, 'p1', null, 's', 'v0', { '': sampleArrayIndex })).toEqual(
  //         {
  //           counts: [ 3, 2, 2 ],
  //           sampleRelations: [[ 's1', 'p1', null ], [ 's2', 'p1', null ], [ 's2', 'p1', null ]],
  //         },
  //       );
  //     });
  //   });
  //   describe('sampleJoin', () => {
  //     let operation1: any;
  //     let operation2: any;
  //     let operation3: any;
  //     beforeEach(() => {
  //       operation1 = {
  //         subject: { termType: 'Variable', value: 'v0' },
  //         predicate: { termType: 'NamedNode', value: 'p1' },
  //         object: { termType: 'Variable', value: 'v1' },
  //       };
  //       operation2 = {
  //         subject: { termType: 'NamedNode', value: 's1' },
  //         predicate: { termType: 'Variable', value: 'pv0' },
  //         object: { termType: 'Variable', value: 'v0' },
  //       };
  //       operation3 = {
  //         subject: { termType: 'Variable', value: 'v1' },
  //         predicate: { termType: 'NamedNode', value: 'p1' },
  //         object: { termType: 'Variable', value: 'v2' },
  //       };

  //       // Extensive index to do 'integration' testing of this function
  //       sampleArrayIndex = {
  //         subjects: {
  //           s1: {
  //             p1: [ 'o1', 'o2', 'o5' ],
  //             p2: [ 'o3', 's2', 's3' ],
  //           },
  //           s2: {
  //             p1: [ 'o4', 'o5', 's3' ],
  //             p2: [ 'o1', 'o4' ],
  //           },
  //           s3: {
  //             p1: [ 'o2', 'o4' ],
  //           },
  //         },
  //         predicates: {
  //           p1: {
  //             s3: [ 's2' ],
  //             o1: [ 's1' ],
  //             o2: [ 's3', 's1' ],
  //             o4: [ 's2', 's3' ],
  //             o5: [ 's1', 's2' ],
  //           },
  //           p2: {
  //             s2: [ 's1' ],
  //             s3: [ 's1' ],
  //             o3: [ 's1' ],
  //             o1: [ 's2' ],
  //             o4: [ 's2' ],
  //           },
  //         },
  //         objects: {
  //           s2: {
  //             s1: [ 'p2' ],
  //           },
  //           s3: {
  //             s1: [ 'p2' ],
  //             s2: [ 'p1' ],
  //           },
  //           o1: {
  //             s1: [ 'p1' ],
  //             s2: [ 'p2' ],
  //           },
  //           o2: {
  //             s1: [ 'p1' ],
  //             s3: [ 'p1' ],
  //           },
  //           o3: {
  //             s1: [ 'p2' ],
  //           },
  //           o4: {
  //             s2: [ 'p1', 'p2' ],
  //             s3: [ 'p1' ],
  //           },
  //           o5: {
  //             s1: [ 'p1' ],
  //             s2: [ 'p2' ],
  //           },
  //         },
  //       };
  //     });
  //     // TODO should this include all possible join shapes?
  //     it('should sample star-subject joins', () => {
  //       // Get all triple in triple pattern
  //       const samples: Record<string, string>[] = joinSampler.sampleIndex(
  //         null,
  //         'p2',
  //         null,
  //         Number.POSITIVE_INFINITY,
  //         sampleArrayIndex,
  //       ).map((x: string[]) => {
  //         return { v0: x[0], v2: x[2] };
  //       });

  //       expect(samples).toEqual(
  //         [{ v0: 's1', v2: 's2' }, { v0: 's1', v2: 's3' }, { v0: 's1', v2: 'o3' }, { v0: 's2', v2: 'o1' }, { v0: 's2', v2: 'o4' }],
  //       );
  //       const spyOnSampleArray = jest.spyOn(joinSampler, 'sampleArray').mockReturnValue([ 2, 3 ]);
  //       const output = joinSampler.sampleJoin(2, samples, null, 'p1', null, operation1, 'v0', 's', { '': sampleArrayIndex });
  //       expect(output.sample).toEqual([{ v0: 's1', v1: 'o5', v2: 's2' }, { v0: 's1', v1: 'o1', v2: 's3' }]);
  //       expect(output.selectivity).toBeCloseTo(3);
  //     });

  //     it('should sample object-subject joins', () => {
  //       const samples: Record<string, string>[] = joinSampler.sampleIndex(
  //         null,
  //         'p1',
  //         null,
  //         Number.POSITIVE_INFINITY,
  //         sampleArrayIndex,
  //       ).map((x: string[]) => {
  //         return { v0: x[0], v1: x[2] };
  //       });
  //       expect(samples).toEqual(
  //         [{ v0: 's2', v1: 's3' }, { v0: 's1', v1: 'o1' }, { v0: 's3', v1: 'o2' }, { v0: 's1', v1: 'o2' }, { v0: 's2', v1: 'o4' }, { v0: 's3', v1: 'o4' }, { v0: 's1', v1: 'o5' }, { v0: 's2', v1: 'o5' }],
  //       );
  //       const spyOnSampleArray = jest.spyOn(joinSampler, 'sampleArray').mockReturnValue([ 0 ]);
  //       const output = joinSampler.sampleJoin(1, samples, null, 'p1', null, operation3, 'v1', 's', { '': sampleArrayIndex });

  //       expect(output.sample).toEqual([{ v0: 's2', v1: 's3', v2: 'o2' }]);
  //       expect(output.selectivity).toBeCloseTo(0.25);
  //     });

  //     it('should sample predicate joins', () => {
  //       const samples: Record<string, string>[] = joinSampler.sampleIndex(
  //         's2',
  //         null,
  //         null,
  //         Number.POSITIVE_INFINITY,
  //         sampleArrayIndex,
  //       )
  //         .map((x: string[]) => {
  //           return { pv0: x[1], v1: x[2] };
  //         });
  //       expect(samples).toEqual(
  //         [{ pv0: 'p1', v1: 'o4' }, { pv0: 'p1', v1: 'o5' }, { pv0: 'p1', v1: 's3' }, { pv0: 'p2', v1: 'o1' }, { pv0: 'p2', v1: 'o4' }],
  //       );

  //       const spyOnSampleArray = jest.spyOn(joinSampler, 'sampleArray').mockReturnValue([ 0, 14 ]);
  //       const output = joinSampler.sampleJoin(2, samples, 's1', null, null, operation2, 'pv0', 'p', { '': sampleArrayIndex });

  //       expect(output.sample).toEqual([{ pv0: 'p1', v0: 'o1', v1: 'o4' }, { pv0: 'p2', v0: 's3', v1: 'o4' }]);
  //       expect(output.selectivity).toBeCloseTo(3);
  //     });

  //     it('should sample object star joins', () => {
  //       const samples: Record<string, string>[] = joinSampler.sampleIndex(
  //         null,
  //         'p2',
  //         null,
  //         Number.POSITIVE_INFINITY,
  //         sampleArrayIndex,
  //       ).map((x: string[]) => {
  //         return { v2: x[0], v1: x[2] };
  //       });
  //       expect(samples).toEqual(
  //         [{ v2: 's1', v1: 's2' }, { v2: 's1', v1: 's3' }, { v2: 's1', v1: 'o3' }, { v2: 's2', v1: 'o1' }, { v2: 's2', v1: 'o4' }],
  //       );
  //       const spyOnSampleArray = jest.spyOn(joinSampler, 'sampleArray').mockReturnValue([ 0, 3 ]);
  //       const output = joinSampler.sampleJoin(2, samples, null, 'p1', null, operation1, 'v1', 'o', { '': sampleArrayIndex });
  //       expect(output.sample).toEqual([
  //         { v0: 's2', v1: 's3', v2: 's1' },
  //         { v0: 's3', v1: 'o4', v2: 's2' },
  //       ]);

  //       expect(output.selectivity).toBeCloseTo(0.8);
  //     });

  //     it('should sample subject-object joins', () => {
  //       const samples: Record<string, string>[] = joinSampler.sampleIndex(
  //         null,
  //         'p1',
  //         null,
  //         Number.POSITIVE_INFINITY,
  //         sampleArrayIndex,
  //       ).map((x: string[]) => {
  //         return { v1: x[0], v2: x[2] };
  //       });
  //       expect(samples).toEqual(
  //         [{ v1: 's2', v2: 's3' }, { v1: 's1', v2: 'o1' }, { v1: 's3', v2: 'o2' }, { v1: 's1', v2: 'o2' }, { v1: 's2', v2: 'o4' }, { v1: 's3', v2: 'o4' }, { v1: 's1', v2: 'o5' }, { v1: 's2', v2: 'o5' }],
  //       );
  //       const spyOnSampleArray = jest.spyOn(joinSampler, 'sampleArray').mockReturnValue([ 0, 1 ]);
  //       const output = joinSampler.sampleJoin(2, samples, null, 'p1', null, operation1, 'v1', 'o', { '': sampleArrayIndex });
  //       expect(output.sample).toEqual([
  //         { v0: 's2', v1: 's3', v2: 'o2' },
  //         { v0: 's2', v1: 's3', v2: 'o4' },
  //       ]);
  //       expect(output.selectivity).toBeCloseTo(0.25);
  //     });

  //     it('should sample with triple pattern with two named nodes', () => {
  //       const operationNamedNodes = {
  //         subject: { termType: 'Variable', value: 'v0' },
  //         predicate: { termType: 'NamedNode', value: 'p1' },
  //         object: { termType: 'NamedNode', value: 'o5' },
  //       };
  //       const samples: Record<string, string>[] = joinSampler.sampleIndex(
  //         null,
  //         'p1',
  //         'o2',
  //         Number.POSITIVE_INFINITY,
  //         sampleArrayIndex,
  //       ).map((x: string[]) => {
  //         return { v0: x[0] };
  //       });
  //       expect(samples).toEqual(
  //         [{ v0: 's3' }, { v0: 's1' }],
  //       );
  //       const spyOnSampleArray = jest.spyOn(joinSampler, 'sampleArray').mockReturnValue([ 0, 1 ]);
  //       const output = joinSampler.sampleJoin(1, samples, null, 'p1', 'o5', operationNamedNodes, 'v0', 's', { '': sampleArrayIndex });
  //       expect(output.sample).toEqual([
  //         { v0: 's1' },
  //       ]);
  //       expect(output.selectivity).toBeCloseTo(0.5);
  //     });

  //     it('should sample with samples consisting of more than one triple pattern', () => {
  //       const operationNamedNodes = {
  //         subject: { termType: 'Variable', value: 'v2' },
  //         predicate: { termType: 'NamedNode', value: 'p2' },
  //         object: { termType: 'NamedNode', value: 'o4' },
  //       };

  //       // Sample base relation
  //       const samples: Record<string, string>[] = joinSampler.sampleIndex(
  //         null,
  //         'p2',
  //         null,
  //         Number.POSITIVE_INFINITY,
  //         sampleArrayIndex,
  //       ).map((x: string[]) => {
  //         return { v0: x[0], v2: x[2] };
  //       });

  //       expect(samples).toEqual(
  //         [{ v0: 's1', v2: 's2' }, { v0: 's1', v2: 's3' }, { v0: 's1', v2: 'o3' }, { v0: 's2', v2: 'o1' }, { v0: 's2', v2: 'o4' }],
  //       );
  //       const intermediateOutput = joinSampler.sampleJoin(Number.POSITIVE_INFINITY, samples, null, 'p1', null, operation1, 'v0', 's', { '': sampleArrayIndex });
  //       // Downsize sample for ease of use
  //       const nextSample = intermediateOutput.sample.slice(0, 5);
  //       expect(nextSample).toEqual([
  //         { v0: 's1', v1: 'o1', v2: 's2' },
  //         { v0: 's1', v1: 'o2', v2: 's2' },
  //         { v0: 's1', v1: 'o5', v2: 's2' },
  //         { v0: 's1', v1: 'o1', v2: 's3' },
  //         { v0: 's1', v1: 'o2', v2: 's3' },
  //       ]);
  //       const spyOnSampleArray = jest.spyOn(joinSampler, 'sampleArray').mockReturnValue([ 1 ]);
  //       const output = joinSampler.sampleJoin(1, nextSample, null, 'p2', 'o4', operationNamedNodes, 'v2', 's', { '': sampleArrayIndex });
  //       expect(output.sample).toEqual([{ v0: 's1', v1: 'o2', v2: 's2' }]);
  //       expect(output.selectivity).toBeCloseTo(0.6);
  //     });
  //     it('should correctly sample with given empty sample', () => {
  //       const samples: Record<string, string>[] = [];
  //       const output = joinSampler.sampleJoin(1, samples, null, 'p1', 'null', operation1, 'v0', 's', { '': sampleArrayIndex });
  //       expect(output.sample).toEqual(
  //         [],
  //       );
  //       expect(output.selectivity).toBeCloseTo(0);
  //     });
  //     it('should correctly sample with no candidate joins', () => {
  //       const samples: Record<string, string>[] = joinSampler.sampleIndex(
  //         null,
  //         'p2',
  //         null,
  //         Number.POSITIVE_INFINITY,
  //         sampleArrayIndex,
  //       ).map((x: string[]) => {
  //         return { v0: x[0], v1: x[2] };
  //       });
  //       expect(samples).toEqual(
  //         [{ v0: 's1', v1: 's2' }, { v0: 's1', v1: 's3' }, { v0: 's1', v1: 'o3' }, { v0: 's2', v1: 'o1' }, { v0: 's2', v1: 'o4' }],
  //       );
  //       const output = joinSampler.sampleJoin(1, samples, 's2', null, null, operation1, 'v1', 'p', { '': sampleArrayIndex });
  //       expect(output.sample).toEqual(
  //         [],
  //       );
  //     });
  //   });

  //   describe('bottomUpEnumeration', () => {});
  // });
});
