import { Bus } from '@comunica/core';
import { DataFactory } from 'rdf-data-factory';
import { AlgebraFactory, Algebra } from '@comunica/utils-algebra';
import { ArrayIterator } from 'asynciterator';
import { arrayifyStream } from 'arrayify-stream';
import type * as RDF from '@rdfjs/types';
import { ActorDerivedResourceSelectTriplePattern } from '../lib/ActorDerivedResourceSelectTriplePattern';
import '@comunica/utils-jest';

const DF = new DataFactory();
const AF = new AlgebraFactory(DF);

describe('ActorDerivedResourceSelectTriplePattern', () => {
  let bus: any;
  let actor: ActorDerivedResourceSelectTriplePattern;

  const VAR_S = DF.variable('s');
  const VAR_P = DF.variable('p');
  const VAR_O = DF.variable('o');
  const VAR_G = DF.variable('g');

  const ALICE = DF.namedNode('http://example.org/alice');
  const BOB = DF.namedNode('http://example.org/bob');
  const CAROL = DF.namedNode('http://example.org/carol');
  const DAVID = DF.namedNode('http://example.org/david');

  const FOAF_KNOWS = DF.namedNode('http://xmlns.com/foaf/0.1/knows');
  const RDFS_SEEALSO = DF.namedNode('http://www.w3.org/2000/01/rdf-schema#seeAlso');

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    actor = new ActorDerivedResourceSelectTriplePattern({
      name: 'actor',
      bus,
      derivedResourceCoefficients: { compute: 1, requests: 1, selectivity: 1 },
      mediatorMetadata: <any>{ mediate: jest.fn() },
      mediatorExtractLinks: <any>{ mediate: jest.fn() },
      mediatorMetadataExtract: <any>{ mediate: jest.fn() },
    });
  });

  describe('filterDataToImport', () => {
    let answeredQueryPatterns: Set<Algebra.Pattern>;

    beforeEach(() => {
      answeredQueryPatterns = new Set();
    });

    it('should return undefined when traversal pattern is not in query (traversal-only pattern)', () => {
      const traversalPattern = AF.createPattern(VAR_S, RDFS_SEEALSO, VAR_O, VAR_G);
      const queryPatterns = [AF.createPattern(VAR_S, FOAF_KNOWS, VAR_O, VAR_G)];
      const dataStream = new ArrayIterator<RDF.Quad>([
        DF.quad(ALICE, RDFS_SEEALSO, BOB),
      ]);

      const result = actor.filterDataToImport(
        traversalPattern,
        dataStream,
        queryPatterns,
        answeredQueryPatterns,
      );

      expect(result).toBeUndefined();
      expect(answeredQueryPatterns.size).toBe(0);
    });

    it('should return the original stream directly when query pattern is an exact match (not a specialization)', () => {
      const traversalPattern = AF.createPattern(VAR_S, FOAF_KNOWS, VAR_O, VAR_G);
      const queryPatterns = [AF.createPattern(VAR_S, FOAF_KNOWS, VAR_O, VAR_G)];
      const dataStream = new ArrayIterator<RDF.Quad>([
        DF.quad(ALICE, FOAF_KNOWS, BOB),
        DF.quad(CAROL, FOAF_KNOWS, DAVID),
      ]);

      const result = actor.filterDataToImport(
        traversalPattern,
        dataStream,
        queryPatterns,
        answeredQueryPatterns,
      );

      // Should return the exact same stream instance without wrapping
      expect(result).toBe(dataStream);
      expect(answeredQueryPatterns.has(queryPatterns[0])).toBe(true);
    });

    it('should return undefined when the matching query pattern has already been answered', () => {
      const traversalPattern = AF.createPattern(VAR_S, FOAF_KNOWS, VAR_O, VAR_G);
      const qp = AF.createPattern(VAR_S, FOAF_KNOWS, VAR_O, VAR_G);
      const queryPatterns = [qp];
      answeredQueryPatterns.add(qp);

      const dataStream = new ArrayIterator<RDF.Quad>([
        DF.quad(ALICE, FOAF_KNOWS, BOB),
      ]);

      const result = actor.filterDataToImport(
        traversalPattern,
        dataStream,
        queryPatterns,
        answeredQueryPatterns,
      );

      expect(result).toBeUndefined();
    });

    it('should return a filtered stream when query pattern is a specialization of the traversal pattern', async () => {
      const traversalPattern = AF.createPattern(VAR_S, FOAF_KNOWS, VAR_O, VAR_G);
      const specializedQp = AF.createPattern(ALICE, FOAF_KNOWS, VAR_O, VAR_G);
      const queryPatterns = [specializedQp];

      const quad1 = DF.quad(ALICE, FOAF_KNOWS, BOB);
      const quad2 = DF.quad(CAROL, FOAF_KNOWS, DAVID);
      const dataStream = new ArrayIterator<RDF.Quad>([quad1, quad2]);

      const result = actor.filterDataToImport(
        traversalPattern,
        dataStream,
        queryPatterns,
        answeredQueryPatterns,
      );

      expect(result).toBeDefined();
      expect(result).not.toBe(dataStream);
      expect(answeredQueryPatterns.has(specializedQp)).toBe(true);

      const quads = await arrayifyStream(result!);
      expect(quads).toEqual([quad1]);
    });

    it('should filter for multiple specialized query patterns matching a single traversal pattern', async () => {
      const traversalPattern = AF.createPattern(VAR_S, FOAF_KNOWS, VAR_O, VAR_G);
      const qpAlice = AF.createPattern(ALICE, FOAF_KNOWS, VAR_O, VAR_G);
      const qpCarol = AF.createPattern(CAROL, FOAF_KNOWS, VAR_O, VAR_G);
      const queryPatterns = [qpAlice, qpCarol];

      const quadAlice = DF.quad(ALICE, FOAF_KNOWS, BOB);
      const quadCarol = DF.quad(CAROL, FOAF_KNOWS, DAVID);
      const quadBob = DF.quad(BOB, FOAF_KNOWS, DAVID);
      const dataStream = new ArrayIterator<RDF.Quad>([quadAlice, quadBob, quadCarol]);

      const result = actor.filterDataToImport(
        traversalPattern,
        dataStream,
        queryPatterns,
        answeredQueryPatterns,
      );

      expect(result).toBeDefined();
      expect(answeredQueryPatterns.has(qpAlice)).toBe(true);
      expect(answeredQueryPatterns.has(qpCarol)).toBe(true);

      const quads = await arrayifyStream(result!);
      expect(quads).toEqual([quadAlice, quadCarol]);
    });

    it('should not filter if at least one matching query pattern is not a specialization', async () => {
      const traversalPattern = AF.createPattern(VAR_S, FOAF_KNOWS, VAR_O, VAR_G);
      const qpGeneral = AF.createPattern(VAR_S, FOAF_KNOWS, VAR_O, VAR_G);
      const qpSpecialized = AF.createPattern(ALICE, FOAF_KNOWS, VAR_O, VAR_G);
      const queryPatterns = [qpGeneral, qpSpecialized];

      const dataStream = new ArrayIterator<RDF.Quad>([
        DF.quad(ALICE, FOAF_KNOWS, BOB),
        DF.quad(CAROL, FOAF_KNOWS, DAVID),
      ]);

      const result = actor.filterDataToImport(
        traversalPattern,
        dataStream,
        queryPatterns,
        answeredQueryPatterns,
      );

      expect(result).toBe(dataStream);
      expect(answeredQueryPatterns.has(qpGeneral)).toBe(true);
      expect(answeredQueryPatterns.has(qpSpecialized)).toBe(true);
    });

    it('should correctly filter with remaining unanswered query patterns', async () => {
      const traversalPattern = AF.createPattern(VAR_S, FOAF_KNOWS, VAR_O, VAR_G);
      const qpAlice = AF.createPattern(ALICE, FOAF_KNOWS, VAR_O, VAR_G);
      const qpCarol = AF.createPattern(CAROL, FOAF_KNOWS, VAR_O, VAR_G);
      const queryPatterns = [qpAlice, qpCarol];

      // Mark Alice as already answered
      answeredQueryPatterns.add(qpAlice);

      const quadAlice = DF.quad(ALICE, FOAF_KNOWS, BOB);
      const quadCarol = DF.quad(CAROL, FOAF_KNOWS, DAVID);
      const dataStream = new ArrayIterator<RDF.Quad>([quadAlice, quadCarol]);

      const result = actor.filterDataToImport(
        traversalPattern,
        dataStream,
        queryPatterns,
        answeredQueryPatterns,
      );

      expect(result).toBeDefined();
      expect(answeredQueryPatterns.has(qpCarol)).toBe(true);

      const quads = await arrayifyStream(result!);
      expect(quads).toEqual([quadCarol]);
    });
  });
});
