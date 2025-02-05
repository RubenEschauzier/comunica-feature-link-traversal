import type * as RDF from '@rdfjs/types';
import type { Operation, Pattern } from 'sparqlalgebrajs/lib/algebra';
import { FifoQueue } from './FifoQueue';
import { string } from '@comunica/utils-expression-evaluator';

/**
 * Sampling-based cardinality estimation
 */
export class IndexBasedJoinSampler {
  public budget: number;

  public constructor(budget: number) {
    this.budget = budget;
  }

  public async run(
    tps: Pattern[],
    nSamples: number,
    sampleFn: SampleFn,
    countFn: CountFn,
  ): Promise<IEnumerationOutput> {
    // Get all triples matching triple pattern
    const { sampledTriplePatterns, cardinalities } = await this.sampleTriplePatternsQuery(
      nSamples,
      tps,
      sampleFn,
      countFn,
    );
    return this.bottomUpEnumeration(
      nSamples,
      sampledTriplePatterns,
      cardinalities,
      tps,
      countFn,
      sampleFn,
    );
  }
  
  // TODO: Implement budget-based early quitting. This means track number of sampleFn calls. When doing early stopping, 
  // we should delegate the rest of the joins to the rdf-join bus. We can signal that we don't know by a special value.
  // In addition, we should think about how we can use dccp with partial estimates.
  public async bottomUpEnumeration(
    n: number,
    resultSets: Record<string, RDF.Term>[][],
    cardinalities: number[],
    tps: Pattern[],
    countFn: CountFn,
    sampleFn: SampleFn,
  ): Promise<IEnumerationOutput> {
    // Samples are stored by their two participating expressions like [[1], [2]] joining result set
    // 1 and 2, while [[1,2],[3]] joins result sets of join between 1,2 and result set of 3.
    const samples: Map<string, ISampleResult> = this.initializeSamplesEnumeration(resultSets, cardinalities);
    const problemQueue = new FifoQueue<number[][]>();
    let budgetLeft = this.budget;
    let maxSizeEstimated: number | undefined;

    for (const join of this.joinCombinations(tps.length)) {
      problemQueue.enqueue(join);
    }
    while (!problemQueue.isEmpty()) {
      // The combination of triple patterns we're evaluating
      const combination = problemQueue.dequeue()!;
      // The intermediate result set we use as input sample for join sampling
      const baseSample = samples.get(JSON.stringify(this.sortArray(combination[0])))!;

      // If our base sample has no entries, we stop this branch as this will never give us
      // samples size > 0.
      if (baseSample.sample.length === 0){
        continue;
      }

      // The triple patterns already joined to produce the intermediate result set
      const tpInBaseSample = combination[0].map(x => tps[x]);
      // The triple pattern we join with intermediate result set
      const spog = this.tpToSampleQuery(tps[combination[1][0]]);
      const joinVariable = this.determineJoinVariable(tpInBaseSample, tps[combination[1][0]]);

      // TODO Optional circle back to cross products if we have exhausted all non-cross products
      if (joinVariable.joinLocation === 'c') {
        continue;
      }

      const samplingOutput: ISampleOutput = await this.sampleJoin(
        n,
        baseSample.sample,
        spog[0],
        spog[1],
        spog[2],
        spog[3],
        tps[combination[1][0]],
        joinVariable.joinVariable,
        joinVariable.joinLocation,
        countFn,
        sampleFn,
      );
      
      /**
       * Update samples by multiplying the base sample estimated cardinality with the estimated
       * selectivity
       */
      samples.set(JSON.stringify(this.sortArray(combination.flat())), {
        sample: samplingOutput.sample,
        estimatedCardinality: samplingOutput.selectivity * baseSample.estimatedCardinality,
      });

      budgetLeft -= samplingOutput.sampleCost;
      if (budgetLeft < 0) {
        // If there are still problems left in the queue, our estimates are incomplete.
        // As we iterate over size (small to large) the first element in the queue shows
        // for what size we have complete estimates.
        const nextCombination = problemQueue.peek()?.flat();
        if (nextCombination){
          maxSizeEstimated = nextCombination.length - 1;
        }
        break;
      }
      // Generate new combinations and add to queue
      this.generateNewCombinations(combination, tps.length).map(x => problemQueue.enqueue(x));
    }
    return { estimates: samples, maxSizeEstimated};
  }

  public async sampleJoin(
    n: number,
    samples: Record<string, RDF.Term>[],
    s: RDF.Term | undefined,
    p: RDF.Term | undefined,
    o: RDF.Term | undefined,
    g: RDF.Term | undefined,
    operation: Pattern,
    joinVariable: string,
    joinLocation: 's' | 'p' | 'o' | 'g',
    countFn: CountFn,
    sampleFn: SampleFn,
  ): Promise<ISampleOutput> {
    let sampleCost: number = 0;
    const { counts, sampleRelations } = await this.candidateCounts(
      samples,
      s,
      p,
      o,
      g,
      joinLocation,
      joinVariable,
      countFn,
    );
    const sum: number = counts.reduce((acc, count) => acc += count, 0);
    const ids = this.generateSampleIndexes(sum, n);

    // We iterate over each id, we could also iterate over the sampleRelations and find all indexes associated with a sampleRelation
    const joinSamples: Record<string, RDF.Term>[] = [];
    for (const id of ids) {
      let searchIndex = 0;
      for (const [ i, count ] of counts.entries()) {
        searchIndex += count;
        // If count > id we take the joins associated with this sample
        if (id < searchIndex) {
          // Go back one step
          searchIndex -= count;

          // Find index
          const tripleIndex: number = id - (searchIndex);

          // Sample single quad at index
          const sampled = [ ...sampleFn([ tripleIndex ], ...sampleRelations[i]) ][0];
          sampleCost++;

          // Get the sample triple that was used to find this join candidate
          let relevantSample: Record<string, RDF.Term> = { ...samples[i] };
          relevantSample = this.addQuadToBindings(sampled, relevantSample, operation);
          joinSamples.push(relevantSample);
          break;
        }
      }
    }
    const selectivity = sum > 0 ? sum / samples.length : 0;
    return {
      sample: joinSamples,
      selectivity,
      sampleCost
    };
  }

  public async candidateCounts(samples: Record<string, RDF.Term>[], s: RDF.Term | undefined, p: RDF.Term | undefined, o: RDF.Term | undefined, g: RDF.Term | undefined, joinLocation: 's' | 'p' | 'o' | 'g', joinVariable: string, countFn: CountFn): Promise<ICandidateCounts> {
    const counts: number[] = [];
    const sampleRelations: (RDF.Term | undefined)[][] = [];
    for (const sample of samples) {
      const sampleRelation = this.setSampleRelation(sample, s, p, o, g, joinLocation, joinVariable);
      counts.push(await countFn(...sampleRelation));
      sampleRelations.push(sampleRelation);
    }
    return {
      counts,
      sampleRelations,
    };
  }

  public async sampleTriplePatternsQuery(
    n: number,
    tps: Pattern[],
    sampleFn: SampleFn,
    countFn: CountFn,
  ): Promise<ISampleTpOutput> {
    const cardinalities: number[] = [];
    const sampledTriplePatterns: Record<string, RDF.Term>[][] = [];

    for (const tp of tps) {
      const tpSampleBindings: Record<string, RDF.Term>[] = [];
      const spogSampleQuery = this.tpToSampleQuery(tp);

      const cardinality = await countFn(...spogSampleQuery);
      cardinalities.push(cardinality);

      const indexes = this.generateSampleIndexes(cardinality, n);
      const triples = [ ...sampleFn(indexes, ...spogSampleQuery) ];

      for (const triple of triples) {
        const binding: Record<string, RDF.Term> = {};
        if (spogSampleQuery[0] === undefined) {
          binding[tp.subject.value] = triple.subject;
        }
        if (spogSampleQuery[1] === undefined) {
          binding[tp.predicate.value] = triple.predicate;
        }
        if (spogSampleQuery[2] === undefined) {
          binding[tp.object.value] = triple.object;
        }
        if (spogSampleQuery[3] === undefined) {
          binding[tp.graph.value] = triple.graph;
        }
        tpSampleBindings.push(binding);
      }
      sampledTriplePatterns.push(tpSampleBindings);
    }
    return {
      sampledTriplePatterns,
      cardinalities,
    };
  }

  // Prob use built in when I find it again
  public isVariable(term: RDF.Term) {
    return term.termType == 'Variable';
  }

  public tpToSampleQuery(tp: Pattern): (RDF.Term | undefined)[] {
    const s: RDF.Term | undefined = this.isVariable(tp.subject) ? undefined : tp.subject;
    const p: RDF.Term | undefined = this.isVariable(tp.predicate) ? undefined : tp.predicate;
    const o: RDF.Term | undefined = this.isVariable(tp.object) ? undefined : tp.object;
    const g: RDF.Term | undefined = this.isVariable(tp.graph) ? undefined : tp.graph;
    return [ s, p, o, g ];
  }

  /**
   * Determines the variable name that matches in the join
   * If no variables match the two quad patterns are a cartesian join
   * @param tpsIntermediateResult All triple patterns that are 'in' the intermediate result
   * @param tp2 triple pattern that will be joined into intermediate result
   * @returns
   */
  public determineJoinVariable(tpsIntermediateResult: Operation[], tp2: Operation): IJoinVariable {
    // TODO What to do with two matching variables (cyclic join graphs, see wanderjoin paper) Maybe just fill make an array
    // of join locations and fill in on all locations in the array. By definition all join locations must be variables
    // in the original triple pattern
    const variablesIntermediateResult: Set<string> = new Set();
    for (const termType of [ 'subject', 'predicate', 'object', 'graph' ]) {
      // If the terms in intermediate result triple patterns are variables we add to set of variables
      tpsIntermediateResult.filter(term => this.isVariable(term[termType]))
        .map(x => variablesIntermediateResult.add(x[termType].value));
    }

    if (this.isVariable(tp2.subject) && variablesIntermediateResult.has(tp2.subject.value)) {
      return { joinLocation: 's', joinVariable: tp2.subject.value };
    }
    if (this.isVariable(tp2.predicate) && variablesIntermediateResult.has(tp2.predicate.value)) {
      return { joinLocation: 'p', joinVariable: tp2.predicate.value };
    }
    if (this.isVariable(tp2.object) && variablesIntermediateResult.has(tp2.object.value)) {
      return { joinLocation: 'o', joinVariable: tp2.object.value };
    }
    if (this.isVariable(tp2.graph) && variablesIntermediateResult.has(tp2.graph.value)) {
      return { joinLocation: 'g', joinVariable: tp2.graph.value };
    }

    return { joinLocation: 'c', joinVariable: '' };
  }

  /**
   * Generate all join combinations size 2
   * @param n the number of triple patterns
   */
  public joinCombinations(n: number): number[][][] {
    const combinations = [];
    for (let i = 0; i < n; i++) {
      for (let j = i + 1; j < n; j++) {
        combinations.push([[ i ], [ j ]]);
      }
    }
    return combinations;
  }

  public initializeSamplesEnumeration(
    resultSets: Record<string, RDF.Term>[][],
    cardinalities: number[],
  ) {
    const samples: Map<string, ISampleResult> = new Map();
    for (const [ i, resultSet ] of resultSets.entries()) {
      samples.set(JSON.stringify([ i ]), { sample: resultSet, estimatedCardinality: cardinalities[i] });
    }
    return samples;
  }

  /**
   * Generate all combination arrays possible for adding a single index to the combination
   * @param combination
   * @param n
   * @returns
   */
  public generateNewCombinations(combination: number[][], n: number): number[][][] {
    if (n <= 0) {
      throw new Error('Received non-positive n');
    }
    if (combination.flat() && (combination.length !== 2 || combination[0].length === 0 || combination[1].length === 0)) {
      throw new Error('Combination should contain atleast two elements with length >= 1');
    }
    const used = new Set(combination.flat());
    return [ ...new Array(n).keys() ].filter(i => !used.has(i)).map(x => [ combination.flat(), [ x ]]);
  }

  public setSampleRelation(sample: Record<string, RDF.Term>, s: RDF.Term | undefined, p: RDF.Term | undefined, o: RDF.Term | undefined, g: RDF.Term | undefined, joinLocation: 's' | 'p' | 'o' | 'g', joinVariable: string) {
    if (Object.keys(sample).length === 0) {
      throw new Error('Sample should contain atleast a binding');
    }
    if (joinLocation === 's') {
      return [ sample[joinVariable], p, o, g ];
    }
    if (joinLocation === 'p') {
      return [ s, sample[joinVariable], o, g ];
    }
    if (joinLocation === 'o') {
      return [ s, p, sample[joinVariable], g ];
    }
    if (joinLocation === 'g') {
      return [ s, p, o, sample[joinVariable] ];
    }

    throw new Error('Invalid join variable');
  }

  public addQuadToBindings(quad: RDF.Quad, bindings: Record<string, RDF.Term>, pattern: Pattern) {
    if (this.isVariable(pattern.subject) && !bindings[pattern.subject.value]) {
      bindings[pattern.subject.value] = quad.subject;
    }
    if (this.isVariable(pattern.predicate) && !bindings[pattern.predicate.value]) {
      bindings[pattern.predicate.value] = quad.predicate;
    }
    if (this.isVariable(pattern.object) && !bindings[pattern.object.value]) {
      bindings[pattern.object.value] = quad.object;
    }
    if (this.isVariable(pattern.graph) && !bindings[pattern.graph.value]) {
      bindings[pattern.graph.value] = quad.graph;
    }
    return bindings;
  }

  public sampleArray<T>(array: T[], n: number): T[] {
    if (n > array.length) {
      return array;
    }

    const result = [];
    const arrCopy = [ ...array ];

    for (let i = 0; i < n; i++) {
      const randomIndex = Math.floor(Math.random() * (arrCopy.length - i)) + i;

      // Swap the randomly selected element to the position i
      [ arrCopy[i], arrCopy[randomIndex] ] = [ arrCopy[randomIndex], arrCopy[i] ];

      // Push the selected element to the result array
      result.push(arrCopy[i]);
    }

    return result;
  }

  public generateSampleIndexes(maxIndex: number, nIndexes: number) {
    const indexArray = Array.from({ length: maxIndex }, (_, i) => i);
    return this.sampleArray(indexArray, nIndexes);
  }

  public sortArray(arr: number[]) {
    return [ ...arr ].sort((n1, n2) => n1 - n2);
  }
}

export type ArrayIndex = Record<string, Record<string, Record<string, string[]>>>;

export interface IEnumerationOutput{
  estimates: Map<string, ISampleResult>;
  maxSizeEstimated?: number;
}

export interface ISampleOutput {
  /**
   *
   */
  sample: Record<string, RDF.Term>[];
  /**
   *
   */
  selectivity: number;
  /**
   * Sample cost (equivalent to the number of SampleFn calls)
   */
  sampleCost: number;
}

export interface ISampleResult {
  /**
   *
   */
  sample: Record<string, RDF.Term>[];
  /**
   * Estimated cardanility associated with join combination
   */
  estimatedCardinality: number;
}

export interface IJoinVariable {
  /**
   * What location the join variable occurs in the to join triple pattern
   */
  joinLocation: 's' | 'p' | 'o' | 'g' | 'c';
  /**
   * The string of the variable
   */
  joinVariable: string;
}

export interface ICandidateCounts {
  /**
   * Number of candidates per sampled binding given as input
   */
  counts: number[];
  /**
   * Triple pattern used to look up candidates in index
   */
  sampleRelations: (RDF.Term | undefined)[][];
}

export interface ISampleTpOutput {
  sampledTriplePatterns: Record<string, RDF.Term>[][];
  cardinalities: number[];
}

export type SampleFn = (
  indexes: number[],
  subject?: RDF.Term,
  predicate?: RDF.Term,
  object?: RDF.Term,
  graph?: RDF.Term
) => RDF.Quad[];

export type CountFn = (
  subject?: RDF.Term | undefined,
  predicate?: RDF.Term | undefined,
  object?: RDF.Term | undefined,
  graph?: RDF.Term | undefined,
) => number | Promise<number>;
