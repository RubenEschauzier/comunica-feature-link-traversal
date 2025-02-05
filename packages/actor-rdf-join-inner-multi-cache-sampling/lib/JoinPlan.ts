import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import type { IJoinEntry } from '@comunica/types';

export class JoinPlan {
  // Left and right entries in join
  public left: JoinPlan | undefined;
  public right: JoinPlan | undefined;
  // Size of intermediate results produced by this join
  public estimatedSize: number;

  public entries: Set<number>;
  public cost: number;

  public constructor(
    left: JoinPlan | undefined,
    right: JoinPlan | undefined,
    entries: Set<number>,
    estimatedSize: number,
  ) {
    this.left = left;
    this.right = right;
    this.entries = entries;
    this.estimatedSize = estimatedSize;
    // Calculate cost according to left and right entries
    this.cost = this.calculateCost();
  }

  /**
   * Calculate cost using a simple cost function
   * @returns calculated cost
   */
  public calculateCost(): number {
    // If either left or right is undefined, we are triple pattern
    if (!this.left || !this.right) {
      return this.estimatedSize;
    }
    const joinCost = (this.left.estimatedSize * this.right.estimatedSize) + this.estimatedSize;
    return joinCost + this.left.cost + this.right.cost;
  }

  /**
   * Joins together join entries based on the plan defined in this class.
   * @param joinEntries All entries that needs to be joined
   * @returns
   */
  public async executePlan(
    joinEntries: IJoinEntry[],
    action: IActionRdfJoin,
    join: (entry1: IJoinEntry, entry2: IJoinEntry, action: IActionRdfJoin) => Promise<IJoinEntry>,
  ) {
    // If either left or right are undefined we are at triple pattern level, so the 'join result'
    // is just the entry associated with the triple pattern
    if (this.left === undefined || this.right === undefined) {
      if (this.entries.size > 1) {
        console.log(this.visualize());
        throw new Error('Left or right are undefined while entries has more than one element');
      }
      return joinEntries[[ ...this.entries ][0]];
    }
    const joinResultLeftTree: IJoinEntry = await this.left.executePlan(joinEntries, action, join);
    const joinResultRightTree: IJoinEntry = await this.right.executePlan(joinEntries, action, join);
    return await join(joinResultLeftTree, joinResultRightTree, action);
  }

  /**
   * Visualize the join plan
   */
  public visualize(level = 0): string {
    const indent = '  '.repeat(level);
    let result = `${indent}Join Node (Size: ${this.estimatedSize}, Cost: ${this.cost})\n`;
    result += `${indent}Entries: [${[ ...this.entries ].join(', ')}]\n`;

    if (this.left) {
      result += `${indent}Left:\n${this.left.visualize(level + 1)}`;
    } else {
      result += `${indent}Left: None\n`;
    }

    if (this.right) {
      result += `${indent}Right:\n${this.right.visualize(level + 1)}`;
    } else {
      result += `${indent}Right: None\n`;
    }

    return result;
  }
}
