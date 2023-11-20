import type { ILink, ILinkQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
// /**
//  * A link queue in order of highest priority.
//  */

// IMPLEMENT DEPTH - BREATH FIRST BY CHECKING IF PARENT URL HAS BEEN IN QUEUE
// IF YES TAKE PARENT PRIORITY += 1
// ELSE PRIORITY = 0
// DO THIS USING WRAPPER AROUND PRIORITY QUEUE LIKE IN LIMIT COUNT
export class LinkQueuePriority implements ILinkQueue {
  public readonly links: ILinkPriority[] = [];
  // We stores the nodes in heap by reference
  public readonly nodesInHeap: Record<string, ILinkPriority> = {};

  public push(link: ILinkPriority): boolean {
    this.links.push(link);
    const idx: number = this.links.length - 1;
    this.upHeap(idx);
    return true;
  }

  public pop(): ILinkPriority {
    const max = this.links[0];
    const endArray = this.links.pop();
    if (this.links.length > 0) {
      this.links[0] = endArray!;
      this.downHeap(0);
    }
    return max;
  }

  /**
   * Function to increase priority of element of heap. First we increase priority using
   * the by reference records. Then we reheap our array. This increases priority in log n
   * time
   */
  public increasePriority(idx: number, increaseBy: number): void {
    if (!this.links[idx] || this.links[idx].priority === undefined) {
      throw new Error(`Access invalid ILinkPriority in heap: ${this.links[idx]?.url}, ${this.links[idx]?.priority}`);
    }
    if (increaseBy <= 0) {
      throw new Error(`Can only increase priority of links by non-zero postive number`);
    }
    this.links[idx].priority += increaseBy;
    this.upHeap(idx);
  }

  public decreasePriority(idx: number, decreaseBy: number): void {
    if (!this.links[idx] || this.links[idx].priority === undefined) {
      throw new Error(`Access invalid ILinkPriority in heap: ${this.links[idx]?.url}, ${this.links[idx]?.priority}`);
    }
    if (decreaseBy <= 0) {
      throw new Error(`Can only decrease priority of links by non-zero postive number`);
    }
    this.links[idx].priority += -decreaseBy;
    this.downHeap(idx);
  }

  public upHeap(idx: number): void {
    if (idx < 0 || idx > this.links.length - 1) {
      throw new Error(`Invalid index passed to upheap in priority queue`);
    }
    if (idx === 0 && !this.links[idx].index) {
      this.links[idx].index = 0;
    }
    const element: ILinkPriority = this.links[idx];
    while (idx > 0) {
      const parentIdx = Math.floor((idx - 1) / 2);
      const parent = this.links[parentIdx];
      if (element.priority <= parent.priority) {
        element.index = idx;
        break;
      }
      // This might break due to by reference stuff, so check for it
      this.links[parentIdx] = element;
      // Update indices
      element.index = parentIdx;
      this.links[idx] = parent;
      parent.index = idx;
      idx = parentIdx;
    }
  }

  public downHeap(idx: number): void {
    if (idx < 0 || idx > this.links.length - 1) {
      throw new Error(`Invalid index passed to upheap in priority queue`);
    }

    const length = this.links.length;
    const element = this.links[idx];
    let performedSwap = false;
    let keepSwapping = true;

    while (keepSwapping) {
      const leftChildIdx = 2 * idx + 1;
      const rightChildIdx = 2 * idx + 2;
      let leftChild,
          rightChild;
      let swap = null;

      // If there exist a left/right child we do comparison
      if (leftChildIdx < length) {
        leftChild = this.links[leftChildIdx];
        if (leftChild.priority > element.priority) {
          swap = leftChildIdx;
        }
      }
      if (rightChildIdx < length) {
        rightChild = this.links[rightChildIdx];
        // Only swap with right child if we either: don't swap a left child and the right child has higher
        // priority or if we do swap and left child has lower priority than right
        if (
          swap === null && rightChild.priority > element.priority ||
          swap !== null && leftChild && rightChild.priority > leftChild.priority
        ) {
          swap = rightChildIdx;
        }
      }
      if (swap === null) {
        // If we don't perform any swap operations we update index
        if (!performedSwap) {
          element.index = idx;
        }
        // This is only for linter..
        keepSwapping = false;
        break;
      }
      performedSwap = true;
      // We swap the elements and their stored indexes
      this.links[idx] = this.links[swap];
      this.links[idx].index = idx;
      this.links[swap] = element;
      this.links[swap].index = swap;
      idx = swap;
    }
  }

  public getSize(): number {
    return this.links.length;
  }

  public isEmpty(): boolean {
    return this.links.length === 0;
  }

  public peek(): ILinkPriority | undefined {
    return this.links[0];
  }
}

export interface ILinkPriority extends ILink{
  /**
   * Priority associated with link
   */
  priority: number;
  /**
   * Index in heap, this is tracked internally
   */
  index?: number;
}
