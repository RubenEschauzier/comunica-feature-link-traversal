import type { Operation } from 'sparqlalgebrajs/lib/algebra';
import type { IEnumerationOutput } from './IndexBasedJoinSampler';
import { JoinPlan } from './JoinPlan';

export class JoinOrderEnumerator {
  private readonly adjacencyList: Map<number, number[]>;
  private readonly estimates: IEnumerationOutput;
  private readonly entries: Operation[];

  public constructor(
    adjacencyList: Map<number, number[]>,
    estimates: IEnumerationOutput,
    entries: Operation[],
  ) {
    this.adjacencyList = adjacencyList;
    this.estimates = estimates;
    this.entries = entries;
  }

  public search(): JoinPlan {
    const bestPlan: Map<string, JoinPlan> = new Map();
    let bestPlanAtMaxSize: JoinPlan = new JoinPlan(
      undefined, undefined, new Set(), Number.POSITIVE_INFINITY
    );

    // If there is a limit to the number of valid estimate we have.
    const maxSize: number = this.estimates.maxSizeEstimated ? 
      this.estimates.maxSizeEstimated : this.entries.length;

    for (let i = 0; i < this.entries.length; i++) {
      const singleton = new Set([ i ]);
      const singletonKey = JSON.stringify(JoinOrderEnumerator.sortArrayAsc([ ...singleton ]));
      bestPlan.set(singletonKey, new JoinPlan(undefined, undefined, singleton, 
        this.estimates.estimates.get(JSON.stringify([ i ]))!.estimatedCardinality));
    }

    const csgCmpPairs = this.enumerateCsgCmpPairs(this.entries.length);
    for (const csgCmpPair of csgCmpPairs) {
      if (csgCmpPair[0].size + csgCmpPair[1].size <= maxSize){
        const tree1 = bestPlan.get(JSON.stringify(JoinOrderEnumerator.sortArrayAsc([ ...csgCmpPair[0] ])))!;
        const tree2 = bestPlan.get(JSON.stringify(JoinOrderEnumerator.sortArrayAsc([ ...csgCmpPair[1] ])))!;
        const newEntries = new Set([ ...tree1.entries, ...tree2.entries ]);
        const estimate = this.estimates.estimates.get(JSON.stringify(JoinOrderEnumerator.sortArrayAsc([ ...newEntries ])));
  
        // If there is no estimated value for entry it means cardinality estimation found 0 estimated cardinality
        let estimatedSize = 0;
        if (estimate){
          estimatedSize = estimate.estimatedCardinality
        }
  
        // The order in which joins happen matters for the cost, so evaluate both
        const currPlanLeft = new JoinPlan(tree1, tree2, newEntries, estimatedSize);
        const currPlanRight = new JoinPlan(tree2, tree1, newEntries, estimatedSize);
        const currPlan = currPlanLeft.cost > currPlanRight.cost ? currPlanRight : currPlanLeft;
  
        const currPlanKey = JSON.stringify(JoinOrderEnumerator.sortArrayAsc([ ...currPlan.entries ]));
  
        if (!bestPlan.get(currPlanKey) || bestPlan.get(currPlanKey)!.cost > currPlan.cost) {
          bestPlan.set(currPlanKey, currPlan);
          if (this.estimates.maxSizeEstimated !== undefined){
            if (currPlan.entries.size === this.estimates.maxSizeEstimated &&
              currPlan.cost < bestPlanAtMaxSize.cost
            ){
              bestPlanAtMaxSize = currPlan;
            }
          }
        }  
      }
    }
    if (this.estimates.maxSizeEstimated){
      return bestPlanAtMaxSize;
    }
    const allEntriesKey = JSON.stringify([ ...Array.from({ length: this.entries.length }).keys() ]);
    return bestPlan.get(allEntriesKey)!;
  }

  public * enumerateCsgCmpPairs(nTps: number) {
    const allTps = new Set(new Array(nTps).keys());
    const csgs = this.enumerateCsg(nTps);
    const csgCmpPairs: [Set<number>, Set<number>][] = [];

    for (const csg of csgs) {
      const cmps = this.enumerateCmp(csg, allTps);
      for (const cmp of cmps) {
        yield [ csg, cmp ];
      }
    }
    return;
  }

  // In this function we should not push a csg cmp pair if we don't have a cardinality estimate
  // This way we only enumerate untill what we know. Either we choose untill all size where we have all samples
  // of size n, or untill size n + 1 with the +1 partial
  public enumerateCsg(nTps: number) {
    const csgs: Set<number>[] = [];
    for (let i = nTps - 1; i >= 0; i--) {
      const vI = new Set([ i ]);
      csgs.push(vI);
      this.enumerateCsgRecursive(csgs, vI, new Set(new Array(i + 1).keys()));
    }
    return csgs;
  }

  public enumerateCmp(tpsSubset: Set<number>, allTps: Set<number>) {
    const cmps: Set<number>[] = [];
    const X = new Set([ ...this.reduceSet(this.setMinimum(tpsSubset), allTps), ...tpsSubset ]);
    const neighbours = this.getNeighbours(tpsSubset);
    for (const vertex of X) {
      neighbours.delete(vertex);
    }

    for (const vertex of JoinOrderEnumerator.sortSetDesc(neighbours)) {
      cmps.push(new Set([ vertex ]));
      this.enumerateCsgRecursive(
        cmps,
        new Set([ vertex ]),
        new Set([ ...X, ...this.reduceSet(vertex, neighbours) ]),
      );
    }
    return cmps;
  }

  public enumerateCsgRecursive(csgs: Set<number>[], S: Set<number>, X: Set<number>) {
    // Difference of neighbours and X
    const neighbours = this.getNeighbours(S);
    for (const vertex of X) {
      neighbours.delete(vertex);
    }

    const subsets = this.getAllSubsets(neighbours);
    for (const subset of subsets) {
      // Add union of two sets as connected subgraph
      csgs.push(new Set([ ...S, ...subset ]));
    }
    for (const subset of subsets) {
      this.enumerateCsgRecursive(csgs, new Set([ ...S, ...subset ]), new Set([ ...X, ...neighbours ]));
    }
  }

  public getNeighbours(S: Set<number>) {
    const neighbours: Set<number> = new Set();
    S.forEach((vertex: number) => {
      this.adjacencyList.get(vertex)!.forEach((neighbour: number) => {
        neighbours.add(neighbour);
      });
    });
    return neighbours;
  }

  public getAllSubsets(set: Set<number>): Set<number>[] {
    const subsets: Set<number>[] = [ new Set([]) ];
    for (const el of set) {
      const last = subsets.length - 1;
      for (let i = 0; i <= last; i++) {
        subsets.push(new Set([ ...subsets[i], el ]));
      }
    }
    // Remove empty set
    return subsets.slice(1);
  }

  public reduceSet(i: number, set: Set<number>) {
    const reducedSet: Set<number> = new Set();
    for (const vertex of set) {
      if (vertex <= i) {
        reducedSet.add(vertex);
      }
    }
    return reducedSet;
  }

  public setMinimum(set: Set<number>): number {
    return Math.min(...set);
  }

  public static sortSetDesc(set: Set<number>): number[] {
    return [ ...set ].sort((a, b) => b - a);
  }

  public static sortArrayAsc(arr: number[]) {
    return [ ...arr ].sort((n1, n2) => n1 - n2);
  }
}