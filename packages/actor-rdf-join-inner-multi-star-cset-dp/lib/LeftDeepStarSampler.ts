import { IJoinEntry } from "@comunica/types";

export async function dpSub(
    entries: IJoinEntry[],
    estimateCardinality: (entries: IJoinEntry[]) => number
): Promise<IEnumerationOutput> {
    const n = entries.length;

    if (n >= 31){
        throw new Error("Cannot enumerate more than 32 entries due to 32-bit integer masks");
    }

    const maxNumBitmask = 1 << n;

    // Initialize dpTable
    const bestPlanCost = new Array<number | undefined>(maxNumBitmask).fill(undefined);
    const bestPlanOrder = new Array<number[] | undefined>(maxNumBitmask).fill(undefined);
    const planCardinality = new Array<number | undefined>(maxNumBitmask).fill(undefined);

    for (let k = 0; k < n; k++){
        const bitMask = 1 << k;
        bestPlanCost[bitMask] = 0;
        bestPlanOrder[bitMask] = [k];
        planCardinality[bitMask] = (await entries[k].output.metadata()).cardinality.value;
    }

    for (let i = 0; i < maxNumBitmask; i++){
        if (numSet(i) <= 1) { 
            continue;
        }
        if (planCardinality[i] === undefined){
            const indexes = getIndexesSet(i);
            const planEntries = indexes.map((i) => entries[i]);
            planCardinality[i] = estimateCardinality(planEntries);
        }

        let tempMask = i;
        while (tempMask > 0){
            // Get right-most bit by inverting applying negative, which
            // inverts the mask and adds 1 to the right-most 0 value. 
            // When we and this we get right-most bit.
            const rightMostBit = tempMask & -tempMask;

            const leftPlanMask = i ^ rightMostBit;
            const rightPlanMask = rightMostBit;
            if (bestPlanCost[leftPlanMask] !== undefined 
                && bestPlanCost[rightPlanMask] !== undefined){
                if (bestPlanCost[rightPlanMask] !== 0){
                    console.log(bestPlanCost[rightPlanMask]);
                    throw new Error(`Found non zero cost ${bestPlanCost[rightPlanMask]} 
                        for right-part of left-deep plan`);
                }

                const cost = (planCardinality[leftPlanMask]! * planCardinality[rightPlanMask]!) 
                    + planCardinality[i]! + bestPlanCost[leftPlanMask];

                if (cost < (bestPlanCost[i] ?? Number.POSITIVE_INFINITY)){
                    bestPlanCost[i] = cost;
                    // Need to sort the join order to ensure smallest entry
                    // is left
                    if (bestPlanOrder[leftPlanMask]?.length === 1 &&
                        planCardinality[leftPlanMask]! > planCardinality[rightPlanMask]!
                    ){
                        bestPlanOrder[i] = [
                            ...bestPlanOrder[rightPlanMask]!, 
                            ...bestPlanOrder[leftPlanMask]!
                        ];
                        continue;
                    }

                    bestPlanOrder[i] = [
                        ...bestPlanOrder[leftPlanMask]!, 
                        ...bestPlanOrder[rightPlanMask]!
                    ];
                }
            }
            tempMask &= tempMask - 1;
        }
    }

    const bestPlan = bestPlanOrder[bestPlanOrder.length - 1]!;
    const stepCardinalities: number[] = [];
    let currentMask = 0;

    for (const nextIdx of bestPlan) {
        // Add the current relation index to the running bitmask
        currentMask |= 1 << nextIdx;
        
        // Retrieve the cardinality for this specific subplan state
        stepCardinalities.push(planCardinality[currentMask]!);
    }

    return {
        plan: bestPlanOrder[bestPlanOrder.length - 1]!,
        cost: bestPlanCost[bestPlanCost.length - 1]!,
        stepCardinalities,
    }
}

export function numSet(bitMask: number){
    let count = 0
    let current = bitMask
    while (current !== 0){
        count++
        current = current & (current - 1);
    }
    return count;
}

export function getIndexesSet(bitMask: number){
    const indices: number[] = [];
    let currentMask = bitMask;

    while (currentMask > 0) {
        // Isolate the right-most set bit using two's complement
        const rightMostBit = currentMask & -currentMask;

        // Calculate the exact 0-based index using Count Leading Zeros
        const index = 31 - Math.clz32(rightMostBit);
        indices.push(index);

        // Clear the right-most set bit to advance the loop
        currentMask &= currentMask - 1;
    }

    return indices;
}

export function bitMaskToString(bitMask: number){
    return bitMask.toString(2);
}

export interface IEnumerationOutput {
    plan: number[];
    cost: number;
    stepCardinalities: number[];
}