import { AsyncIterator } from 'asynciterator';

/**
 * Joins two items, or returns null if they are incompatible.
 */
export type JoinFunction<T> = (left: T, right: T) => T | null;

/**
 * Computes the key under which an item is stored and probed. Items can only join when their keys are
 * equal; a constant key turns the join into a cartesian product.
 */
export type KeyFunction<T> = (item: T) => string;

interface ILeftEntry<T> {
  item: T;
  /**
   * If the item has joined with a right item, or has already been emitted on its own.
   */
  matched: boolean;
}

interface IProbe<T> {
  item: T;
  fromLeft: boolean;
  /**
   * The stored entry of a left item, if it was stored.
   */
  leftEntry?: ILeftEntry<T>;
  /**
   * The stored items of the other side under the same key, and how many of them existed when this
   * item arrived. Items stored later join with this one when they arrive themselves.
   */
  candidates: (T | ILeftEntry<T>)[];
  end: number;
  index: number;
  matched: boolean;
}

/**
 * A symmetric hash join of two streams, optionally as a left join (OPTIONAL).
 *
 * Every item is stored under its key and immediately joined with the stored items of the other side,
 * so results are produced as soon as both of their items have arrived, in any order, and neither side
 * has to end first. Each pair of items is joined exactly once, by whichever of the two arrived last.
 *
 * Nested-loop and hash joins that go through a MultiTransformIterator emit per left item in order, and
 * move on to the next left item only once the right side has ended. Over link traversal no stream ends
 * before traversal does, so those joins hold back almost all results until then.
 *
 * As a left join, a left item that never joins is emitted on its own once the right side has ended.
 * That is inherent to OPTIONAL: before the right side has ended, a later right item could still match.
 */
export class SymmetricJoinIterator<T> extends AsyncIterator<T> {
  private readonly leftBuckets = new Map<string, ILeftEntry<T>[]>();
  private readonly rightBuckets = new Map<string, T[]>();
  /**
   * All stored left entries in arrival order, to find the unmatched ones of a left join.
   */
  private readonly leftEntries: ILeftEntry<T>[] = [];
  private unmatchedCursor = 0;
  private readLeftNext = true;
  private probe: IProbe<T> | undefined;

  public constructor(
    private readonly left: AsyncIterator<T>,
    private readonly right: AsyncIterator<T>,
    private readonly funKey: KeyFunction<T>,
    private readonly funJoin: JoinFunction<T>,
    private readonly optional: boolean,
  ) {
    super();
    for (const source of [ left, right ]) {
      source.on('readable', () => {
        this.readable = true;
      });
      source.on('end', () => {
        this.readable = true;
      });
      source.on('error', (error: Error) => this.destroy(error));
    }
    if (left.readable || right.readable || left.done || right.done) {
      this.readable = true;
    }
  }

  public override read(): T | null {
    while (!this.done) {
      // Join the current item with the stored items of the other side
      const probe = this.probe;
      if (probe) {
        while (probe.index < probe.end) {
          const candidate = probe.candidates[probe.index++];
          const result = probe.fromLeft ?
            this.funJoin(probe.item, <T> candidate) :
            this.funJoin((<ILeftEntry<T>> candidate).item, probe.item);
          if (result !== null) {
            probe.matched = true;
            if (probe.fromLeft) {
              if (probe.leftEntry) {
                probe.leftEntry.matched = true;
              }
            } else {
              (<ILeftEntry<T>> candidate).matched = true;
            }
            return result;
          }
        }
        this.probe = undefined;
        // A left item that arrived after the right side ended, and so was not stored, has seen every right
        // item; if it joined with none, it is complete on its own. A stored one that never joins is
        // emitted by the sweep below once the right side has ended
        if (this.optional && probe.fromLeft && !probe.matched && !probe.leftEntry) {
          return probe.item;
        }
        continue;
      }

      // Once the right side has ended, the stored left items that never joined are complete on their own
      if (this.optional && this.right.done) {
        while (this.unmatchedCursor < this.leftEntries.length) {
          const entry = this.leftEntries[this.unmatchedCursor++];
          if (!entry.matched) {
            entry.matched = true;
            return entry.item;
          }
        }
      }

      // Read the next item, alternating between both sides
      let item: T | null = null;
      let fromLeft = false;
      for (let attempt = 0; attempt < 2 && item === null; attempt++) {
        fromLeft = this.readLeftNext;
        this.readLeftNext = !this.readLeftNext;
        const source = fromLeft ? this.left : this.right;
        if (!source.done) {
          item = source.read();
        }
      }
      if (item === null) {
        if (this.left.done && this.right.done) {
          this.close();
        } else {
          this.readable = false;
        }
        return null;
      }

      const key = this.funKey(item);
      if (fromLeft) {
        // A left item only has to be stored for right items that are still to come
        let leftEntry: ILeftEntry<T> | undefined;
        if (!this.right.done) {
          leftEntry = { item, matched: false };
          SymmetricJoinIterator.store(this.leftBuckets, key, leftEntry);
          this.leftEntries.push(leftEntry);
        }
        const candidates = this.rightBuckets.get(key) ?? [];
        this.probe = { item, fromLeft, leftEntry, candidates, end: candidates.length, index: 0, matched: false };
      } else {
        if (!this.left.done) {
          SymmetricJoinIterator.store(this.rightBuckets, key, item);
        }
        const candidates = this.leftBuckets.get(key) ?? [];
        this.probe = { item, fromLeft, candidates, end: candidates.length, index: 0, matched: false };
      }
    }
    return null;
  }

  private static store<V>(buckets: Map<string, V[]>, key: string, value: V): void {
    let bucket = buckets.get(key);
    if (!bucket) {
      bucket = [];
      buckets.set(key, bucket);
    }
    bucket.push(value);
  }

  protected override _end(destroy = false): void {
    super._end(destroy);
    this.left.destroy();
    this.right.destroy();
    this.leftBuckets.clear();
    this.rightBuckets.clear();
    this.leftEntries.length = 0;
    this.probe = undefined;
  }
}
