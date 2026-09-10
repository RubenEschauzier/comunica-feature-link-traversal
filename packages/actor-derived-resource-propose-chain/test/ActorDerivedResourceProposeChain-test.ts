import { Bus } from '@comunica/core';
import { DataFactory } from 'rdf-data-factory';
import { AlgebraFactory, Algebra } from '@comunica/utils-algebra';
import { ActorDerivedResourceProposeChain, ISubChainSplit } from '../lib/ActorDerivedResourceProposeChain';
import '@comunica/utils-jest';

const DF = new DataFactory();
const AF = new AlgebraFactory(DF);

/**
 * A linear chain of `length` patterns: ?v0 <p0> ?v1 . ?v1 <p1> ?v2 . ...
 */
function createChain(length: number): Algebra.Pattern[] {
  const patterns: Algebra.Pattern[] = [];
  for (let i = 0; i < length; i++) {
    patterns.push(AF.createPattern(
      DF.variable(`v${i}`),
      DF.namedNode(`http://example.org/p${i}`),
      DF.variable(`v${i + 1}`),
    ));
  }
  return patterns;
}

describe('ActorDerivedResourceProposeChain', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('enumerateSubChains', () => {
    /**
     * A split holds the parts of maxChainLength as `chunks` and the parts that are too short as
     * `leftOvers`, both in chain order and both as indexes into the chain. Leftovers get one part
     * per run, so adjacent leftovers stay together and separated ones do not.
     * The splits themselves are compared irrespective of the order they are enumerated in.
     */
    function enumerate(chainLength: number, maxChainLength: number): ISubChainSplit[] {
      const actor = new ActorDerivedResourceProposeChain(<any>{ name: 'actor', bus, maxChainLength });
      const splits: ISubChainSplit[] = (<any>actor).enumerateSubChains(createChain(chainLength), maxChainLength);
      return [ ...splits ].sort((left, right) => {
        const leftKey = JSON.stringify(left);
        const rightKey = JSON.stringify(right);
        return leftKey < rightKey ? -1 : (leftKey > rightKey ? 1 : 0);
      });
    }

    describe('with maxChainLength 2', () => {
      it('should place the single leftover at either end of a chain of 3', () => {
        expect(enumerate(3, 2)).toEqual([
          { chunks: [[ 0, 1 ]], leftOvers: [[ 2 ]]},
          { chunks: [[ 1, 2 ]], leftOvers: [[ 0 ]]},
        ]);
      });

      it('should place the single leftover at every part boundary of a chain of 5', () => {
        expect(enumerate(5, 2)).toEqual([
          { chunks: [[ 0, 1 ], [ 2, 3 ]], leftOvers: [[ 4 ]]},
          { chunks: [[ 0, 1 ], [ 3, 4 ]], leftOvers: [[ 2 ]]},
          { chunks: [[ 1, 2 ], [ 3, 4 ]], leftOvers: [[ 0 ]]},
        ]);
      });
    });

    describe('with maxChainLength 3', () => {
      it('should place the single leftover at either end of a chain of 4', () => {
        expect(enumerate(4, 3)).toEqual([
          { chunks: [[ 0, 1, 2 ]], leftOvers: [[ 3 ]]},
          { chunks: [[ 1, 2, 3 ]], leftOvers: [[ 0 ]]},
        ]);
      });

      it('should place the single leftover at every part boundary of a chain of 7', () => {
        expect(enumerate(7, 3)).toEqual([
          { chunks: [[ 0, 1, 2 ], [ 3, 4, 5 ]], leftOvers: [[ 6 ]]},
          { chunks: [[ 0, 1, 2 ], [ 4, 5, 6 ]], leftOvers: [[ 3 ]]},
          { chunks: [[ 1, 2, 3 ], [ 4, 5, 6 ]], leftOvers: [[ 0 ]]},
        ]);
      });
    });

    describe('with two leftovers', () => {
      it('should enumerate every pair of leftovers of a chain of 5 with maxChainLength 3', () => {
        expect(enumerate(5, 3)).toEqual([
          { chunks: [[ 0, 1, 2 ]], leftOvers: [[ 3, 4 ]]},
          { chunks: [[ 1, 2, 3 ]], leftOvers: [[ 0 ], [ 4 ]]},
          { chunks: [[ 2, 3, 4 ]], leftOvers: [[ 0, 1 ]]},
        ]);
      });

      it('should keep leftovers that are not adjacent apart in a chain of 8 with maxChainLength 3', () => {
        expect(enumerate(8, 3)).toEqual([
          { chunks: [[ 0, 1, 2 ], [ 3, 4, 5 ]], leftOvers: [[ 6, 7 ]]},
          { chunks: [[ 0, 1, 2 ], [ 4, 5, 6 ]], leftOvers: [[ 3 ], [ 7 ]]},
          { chunks: [[ 0, 1, 2 ], [ 5, 6, 7 ]], leftOvers: [[ 3, 4 ]]},
          { chunks: [[ 1, 2, 3 ], [ 4, 5, 6 ]], leftOvers: [[ 0 ], [ 7 ]]},
          { chunks: [[ 1, 2, 3 ], [ 5, 6, 7 ]], leftOvers: [[ 0 ], [ 4 ]]},
          { chunks: [[ 2, 3, 4 ], [ 5, 6, 7 ]], leftOvers: [[ 0, 1 ]]},
        ]);
      });
    });

    describe('with three leftovers', () => {
      it('should enumerate every triple of leftovers of a chain of 7 with maxChainLength 4', () => {
        expect(enumerate(7, 4)).toEqual([
          { chunks: [[ 0, 1, 2, 3 ]], leftOvers: [[ 4, 5, 6 ]]},
          { chunks: [[ 1, 2, 3, 4 ]], leftOvers: [[ 0 ], [ 5, 6 ]]},
          { chunks: [[ 2, 3, 4, 5 ]], leftOvers: [[ 0, 1 ], [ 6 ]]},
          { chunks: [[ 3, 4, 5, 6 ]], leftOvers: [[ 0, 1, 2 ]]},
        ]);
      });
    });

    describe('without leftovers', () => {
      it('should produce a single split for a chain that is exactly one part', () => {
        expect(enumerate(3, 3)).toEqual([
          { chunks: [[ 0, 1, 2 ]], leftOvers: []},
        ]);
      });

      it('should produce a single split for a chain of an exact multiple of maxChainLength', () => {
        expect(enumerate(6, 3)).toEqual([
          { chunks: [[ 0, 1, 2 ], [ 3, 4, 5 ]], leftOvers: []},
        ]);
      });
    });

    describe('on edge cases', () => {
      it('should produce no splits for an empty chain', () => {
        expect(enumerate(0, 3)).toEqual([]);
      });

      it('should produce a single leftover part for a chain of one', () => {
        expect(enumerate(1, 3)).toEqual([
          { chunks: [], leftOvers: [[ 0 ]]},
        ]);
      });

      it('should keep a chain shorter than maxChainLength together as one leftover part', () => {
        expect(enumerate(2, 3)).toEqual([
          { chunks: [], leftOvers: [[ 0, 1 ]]},
        ]);
      });

      it('should give every pattern its own chunk for maxChainLength 1', () => {
        expect(enumerate(3, 1)).toEqual([
          { chunks: [[ 0 ], [ 1 ], [ 2 ]], leftOvers: []},
        ]);
      });
    });
  });
});
