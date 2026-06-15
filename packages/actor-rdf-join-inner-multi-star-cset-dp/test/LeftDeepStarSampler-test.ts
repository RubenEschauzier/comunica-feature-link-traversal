import { dpSub, numSet, getIndexesSet } from '../lib/LeftDeepStarSampler'; // Adjust path
import type { IJoinEntry } from "@comunica/types";

/**
 * Mocks Comunica's IJoinEntry and its asynchronous metadata.
 */
function createMockEntry(name: string, cardinality: number): IJoinEntry {
    return {
        output: {
            metadata: async () => ({
                cardinality: { type: 'exact', value: cardinality }
            })
        },
        name,
        // Store base cardinality synchronously for the mock estimator
        _baseCardinality: cardinality 
    } as unknown as IJoinEntry;
}

describe('Left-Deep Dynamic Programming Enumerator', () => {

    describe('numSet Utility', () => {
        it('counts the number of active bits', () => {
            expect(numSet(0)).toBe(0); // 0000
            expect(numSet(1)).toBe(1); // 0001
            expect(numSet(3)).toBe(2); // 0011
            expect(numSet(7)).toBe(3); // 0111
        });
    });

    describe('getIndexesSet Utility', () => {
        it('extracts zero-based indices from a bitmask', () => {
            expect(getIndexesSet(0)).toEqual([]);
            expect(getIndexesSet(1)).toEqual([0]);       // 0001
            expect(getIndexesSet(5)).toEqual([0, 2]);    // 0101
            expect(getIndexesSet(10)).toEqual([1, 3]);   // 1010
        });
    });

    describe('dpSub Algorithm', () => {
        it('throws an error if the relation count exceeds the 32-bit integer limit', async () => {
            const entries = Array.from({ length: 31 }, (_, i) => createMockEntry(`T${i}`, 10));
            const estimateCardinality = jest.fn();

            await expect(dpSub(entries, estimateCardinality)).rejects.toThrow(
                "Cannot enumerate more than 32 entries due to 32-bit integer masks"
            );
        });

        it('returns a base plan with zero cost and correct step cardinalities for a single relation', async () => {
            const entries = [createMockEntry('A', 100)];
            const estimateCardinality = jest.fn();

            const result = await dpSub(entries, estimateCardinality);

            expect(result.plan).toEqual([0]);
            expect(result.cost).toBe(0);
            expect(result.stepCardinalities).toEqual([100]);
            expect(estimateCardinality).not.toHaveBeenCalled();
        });

        it('calculates cost, sequence, and step cardinalities for two relations using a sub-star ratio', async () => {
            const entries = [
                createMockEntry('A', 10),
                createMockEntry('B', 20)
            ];
            
            // Calculate intermediate cardinality using a selectivity ratio against the base patterns
            const estimateCardinality = jest.fn((planEntries: any[]) => {
                const maxBase = Math.max(...planEntries.map(e => e._baseCardinality));
                return maxBase * 0.5; // Assume sub-star cardinality is 50% of the largest triple pattern
            });

            const result = await dpSub(entries, estimateCardinality);

            // Cost Calculation:
            // card(A) = 10, card(B) = 20
            // card(A join B) = max(10, 20) * 0.5 = 10
            // Cost = (card(A) * card(B)) + card(A join B) + cost(A)
            // Cost = (10 * 20) + 10 + 0 = 210

            expect(result.plan).toEqual([0, 1]);
            expect(result.cost).toBe(210);
            expect(result.stepCardinalities).toEqual([10, 10]); // [card(A), card(A join B)]
            expect(estimateCardinality).toHaveBeenCalledTimes(1);
        });

        it('generates a valid left-deep sequence for three relations', async () => {
            const entries = [
                createMockEntry('A', 10),
                createMockEntry('B', 10),
                createMockEntry('C', 10)
            ];
            
            // Maintain a constant ratio of 0.5 for all sub-star cardinalities relative to the max base
            const estimateCardinality = jest.fn((planEntries: any[]) => {
                const maxBase = Math.max(...planEntries.map(e => e._baseCardinality));
                return maxBase * 0.5; // Returns 5 for all intermediate joins
            });

            const result = await dpSub(entries, estimateCardinality);

            // Manual Cost Tracing:
            // Base cardinalities = 10. All estimated sub-stars = 5.
            // Step 1: Join {0,1} 
            //    Cost = (10 * 10) + 5 + 0 = 105
            // Step 2: Join {0,1} with {2}
            //    Left card = 5, Right card = 10
            //    Cost = (5 * 10) + 5 (from estimate) + 105 (left plan cost) = 160

            expect(result.plan?.length).toBe(3);
            expect(result.cost).toBe(160);
            expect(result.stepCardinalities).toEqual([10, 5, 5]); 
            
            // Verify permutation completeness
            expect(result.plan).toContain(0);
            expect(result.plan).toContain(1);
            expect(result.plan).toContain(2);
        });
    });
    it(`generates the optimal left-deep sequence for four relations`, async () =>  {
        const entries = [
            createMockEntry('A', 60),
            createMockEntry('B', 70),
            createMockEntry('C', 70),
            createMockEntry('D', 60),
        ];
        const estimateCardinality = jest.fn((planEntries: any[]) => {
            // Extract names, sort them to ensure order-independence, and serialize to a string
            const planEntriesNames = planEntries.map(x => x.name).sort().join(',');
            
            if (planEntriesNames === 'A,C') {
                return 40;
            }
            if (planEntriesNames === 'A,C,D') {
                return 1;
            }
            
            const minBase = Math.min(...planEntries.map(e => e._baseCardinality));
            return minBase;
        });
        const result = await dpSub(entries, estimateCardinality);
        expect(result.plan?.length).toBe(4);
        expect(result.plan).toEqual([0,2,3,1]);
        expect(result.cost).toBe(6771);
        expect(result.stepCardinalities).toEqual([60, 40, 1, 60]);
    });
});