import { IJoinEntry } from "@comunica/types";

export async function dpSub(entries: IJoinEntry[], estimateCardinality: (entries: IJoinEntry[]) => number){
    const n = entries.length;
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
            // TODO Make this get as input the operation of the entry associated with 
            // this subplan
            planCardinality[i] = estimateCardinality()
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
                    bestPlanOrder[i] = [
                        ...bestPlanOrder[leftPlanMask]!, 
                        ...bestPlanOrder[rightPlanMask]!
                    ];
                }
            }
            tempMask &= tempMask - 1;
        }
    }
    return {
        plan: bestPlanOrder[bestPlanOrder.length - 1],
        cost: bestPlanCost[bestPlanCost.length - 1],
        // TODO: Set the step cardinalities so they can be passed to the join optimization 
        // for the correct selection of join algorithm
        stepCardinalities: undefined
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