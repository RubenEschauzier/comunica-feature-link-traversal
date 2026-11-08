import { QuerySourceSparql } from '@comunica/actor-query-source-identify-hypermedia-sparql';
import { ActionContext } from '@comunica/core';
import type { FragmentSelectorShape } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { canShapeAnswerBgp } from '../lib/ShapeMatching';

const DF = new DataFactory();
const AF = new AlgebraFactory(DF);

const knows = AF.createPattern(DF.variable('s'), DF.namedNode('ex:knows'), DF.variable('o'));
const name = AF.createPattern(DF.variable('s'), DF.namedNode('ex:name'), DF.variable('n'));
const chain = AF.createPattern(DF.variable('o'), DF.namedNode('ex:name'), DF.variable('m'));

describe('canShapeAnswerBgp', () => {
  describe('for the shape of a SPARQL endpoint', () => {
    let shape: FragmentSelectorShape;

    beforeAll(async() => {
      // The source is never queried, so what it is given to query with is not needed
      const source = new QuerySourceSparql(
        'http://ex.org/sparql', 'http://ex.org/sparql', new ActionContext(), <any> {}, <any> {}, 'values',
        DF, AF, <any> {}, false, 0, 0, false, false, 0, false, {},
      );
      shape = await source.getSelectorShape();
    });

    it('answers a single pattern', () => {
      expect(canShapeAnswerBgp(shape, AF.createBgp([ knows ]))).toBe(true);
    });

    it('answers a star', () => {
      expect(canShapeAnswerBgp(shape, AF.createBgp([ knows, name ]))).toBe(true);
    });

    it('answers a chain', () => {
      expect(canShapeAnswerBgp(shape, AF.createBgp([ knows, chain ]))).toBe(true);
    });
  });

  describe('for the shape of a QPF interface', () => {
    const shape: FragmentSelectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern: AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o'), DF.variable('g')),
      },
      variablesOptional: [ DF.variable('s'), DF.variable('p'), DF.variable('o'), DF.variable('g') ],
    };

    it('answers no bgp, not even of one pattern', () => {
      expect(canShapeAnswerBgp(shape, AF.createBgp([ knows ]))).toBe(false);
      expect(canShapeAnswerBgp(shape, AF.createBgp([ knows, name ]))).toBe(false);
    });
  });

  describe('for the shape of a parameterized star', () => {
    const shape: FragmentSelectorShape = {
      type: 'operation',
      operation: { operationType: 'pattern', pattern: AF.createBgp([ knows, name ]) },
      variablesOptional: [ DF.variable('s'), DF.variable('o'), DF.variable('n') ],
    };

    it('answers an instance of its template', () => {
      expect(canShapeAnswerBgp(shape, AF.createBgp([ name, knows ]))).toBe(true);
    });

    it('answers no bgp of another size', () => {
      expect(canShapeAnswerBgp(shape, AF.createBgp([ knows ]))).toBe(false);
    });
  });
});
