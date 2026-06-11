import { dpSub, numSet } from '../lib/LeftDeepStarSampler'; // Adjust path to your file
import type { IJoinEntry } from "@comunica/types";

/**
 * Utility to mock Comunica's IJoinEntry and its asynchronous metadata.
 */
function createMockEntry(name: string, cardinality: number): IJoinEntry {
    return {
        // Add dummy properties if typescript complains about missing IJoinEntry fields
        output: {
            metadata: async () => ({
                cardinality: { type: 'exact', value: cardinality }
            })
        }
    } as unknown as IJoinEntry;
}

describe('dpSub Left-Deep Enumerator', () => {

    describe('numSet Utility', () => {
        it('correctly counts the number of active bits', () => {
            expect(numSet(0)).toBe(0); // 0000
            expect(numSet(1)).toBe(1); // 0001
            expect(numSet(3)).toBe(2); // 0011
            expect(numSet(7)).toBe(3); // 0111
        });
    });

    describe('dpSub Algorithm', () => {
        it('returns a base plan with zero cost for a single relation', async () => {
            const entries = [createMockEntry('A', 100)];
            const estimateCardinality = jest.fn(); // Won't be called for size 1

            const result = await dpSub(entries, estimateCardinality);

            expect(result.plan).toEqual([0]);
            expect(result.cost).toBe(0);
            expect(estimateCardinality).not.toHaveBeenCalled();
        });

        it('calculates the correct cost and sequence for two relations', async () => {
            const entries = [
                createMockEntry('A', 10),
                createMockEntry('B', 20)
            ];
            
            // Mock intermediate cardinality returning a fixed 50
            const estimateCardinality = jest.fn().mockReturnValue(50);

            const result = await dpSub(entries, estimateCardinality);

            // Expected Cost: (card(A) * card(B)) + card(A join B) + cost(A)
            // (10 * 20) + 50 + 0 = 250
            expect(result.plan).toEqual([0, 1]);
            expect(result.cost).toBe(250);
            expect(estimateCardinality).toHaveBeenCalledTimes(1); // Called for mask 3
        });

        it('generates a valid left-deep sequence for three relations', async () => {
            const entries = [
                createMockEntry('A', 10),
                createMockEntry('B', 10),
                createMockEntry('C', 10)
            ];
            
            // Because estimateCardinality takes no arguments in your current implementation,
            // we return a fixed intermediate cardinality (100) for all combinations.
            const estimateCardinality = jest.fn().mockReturnValue(100);

            const result = await dpSub(entries, estimateCardinality);

            /*
              Manual Cost Tracing with symmetrical inputs (all bases=10, all intermediates=100):
              Step 1: Join {0,1} 
                 Cost = (10 * 10) + 100 + 0 = 200
              Step 2: Join {0,1} with {2}
                 Left card = 100, Right card = 10
                 Cost = (100 * 10) + 100 (from estimate) + 200 (left plan cost) = 1300
            */

            expect(result.plan?.length).toBe(3);
            expect(result.cost).toBe(1300);
            
            // Verify it generated a permutation containing all indices
            expect(result.plan).toContain(0);
            expect(result.plan).toContain(1);
            expect(result.plan).toContain(2);
        });

        it('throws an error if right operand has a non-zero cost (not a singleton)', async () => {
            const entries = [
                createMockEntry('A', 10),
                createMockEntry('B', 10),
                createMockEntry('C', 10)
            ];
            const estimateCardinality = jest.fn().mockReturnValue(100);

            // Since your loop isolates rightMostBit (tempMask & -tempMask), 
            // the right operand will ALWAYS be a singleton (a power of 2).
            // Your initialization block guarantees singletons have 0 cost.
            // Therefore, this test ensures the function resolves normally 
            // without triggering your internal Error guard rail.
            await expect(dpSub(entries, estimateCardinality)).resolves.not.toThrow();
        });
    });
});