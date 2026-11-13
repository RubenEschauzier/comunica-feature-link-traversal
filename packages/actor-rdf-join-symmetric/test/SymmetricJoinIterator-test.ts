import { ArrayIterator, BufferedIterator } from 'asynciterator';
import { SymmetricJoinIterator } from '../lib/SymmetricJoinIterator';

type IRow = Record<string, string>;

/**
 * Joins two rows, or returns null when they disagree on a shared variable.
 */
function join(left: IRow, right: IRow): IRow | null {
  for (const key of Object.keys(left)) {
    if (key in right && right[key] !== left[key]) {
      return null;
    }
  }
  return { ...left, ...right };
}

/**
 * A stream that stays open until closed, like a pattern stream during link traversal.
 */
function openStream(): BufferedIterator<IRow> & { add: (row: IRow) => void } {
  const stream = <any> new BufferedIterator<IRow>({ autoStart: false });
  stream.add = (row: IRow): void => stream._push(row);
  return stream;
}

function drain(iterator: SymmetricJoinIterator<IRow>): IRow[] {
  const rows: IRow[] = [];
  let row = iterator.read();
  while (row !== null) {
    rows.push(row);
    row = iterator.read();
  }
  return rows;
}

/**
 * Reads everything the join can produce now, over a few turns of the event loop, as a consumer
 * reacting to 'readable' and 'end' events would.
 */
async function settle(iterator: SymmetricJoinIterator<IRow>): Promise<IRow[]> {
  const rows: IRow[] = [];
  for (let turn = 0; turn < 5; turn++) {
    await new Promise(resolve => setImmediate(resolve));
    rows.push(...drain(iterator));
  }
  return rows;
}

function waitForEnd(iterator: SymmetricJoinIterator<IRow>): Promise<IRow[]> {
  return iterator.toArray();
}

const sortRows = (rows: IRow[]): string[] => rows.map(row => JSON.stringify(row)).sort();

describe('SymmetricJoinIterator', () => {
  describe('as a cartesian inner join', () => {
    it('produces all pairs when both sides end', async() => {
      const join2 = new SymmetricJoinIterator<IRow>(
        new ArrayIterator<IRow>([{ a: '1' }, { a: '2' }]),
        new ArrayIterator<IRow>([{ b: 'x' }, { b: 'y' }, { b: 'z' }]),
        () => '',
        join,
        false,
      );
      expect(sortRows(await waitForEnd(join2))).toEqual(sortRows([
        { a: '1', b: 'x' },
        { a: '1', b: 'y' },
        { a: '1', b: 'z' },
        { a: '2', b: 'x' },
        { a: '2', b: 'y' },
        { a: '2', b: 'z' },
      ]));
    });

    it('produces results while neither side has ended', async() => {
      const left = openStream();
      const right = openStream();
      const join2 = new SymmetricJoinIterator<IRow>(left, right, () => '', join, false);
      left.add({ a: '1' });
      left.add({ a: '2' });
      left.add({ a: '3' });
      left.add({ a: '4' });
      left.add({ a: '5' });
      right.add({ b: 'x' });
      // The nested-loop join would emit only { a: 1, b: x } here, and wait for the right side to end
      expect(sortRows(await settle(join2))).toEqual(sortRows([
        { a: '1', b: 'x' },
        { a: '2', b: 'x' },
        { a: '3', b: 'x' },
        { a: '4', b: 'x' },
        { a: '5', b: 'x' },
      ]));
      expect(join2.done).toBe(false);

      right.add({ b: 'y' });
      await expect(settle(join2)).resolves.toHaveLength(5);

      left.close();
      right.close();
      await expect(settle(join2)).resolves.toEqual([]);
      expect(join2.done).toBe(true);
    });

    it('joins right items arriving after the left side ended with the stored left items', async() => {
      const left = openStream();
      const right = openStream();
      const join2 = new SymmetricJoinIterator<IRow>(left, right, () => '', join, false);
      left.add({ a: '1' });
      left.add({ a: '2' });
      left.close();
      await expect(settle(join2)).resolves.toEqual([]);

      right.add({ b: 'x' });
      right.add({ b: 'y' });
      right.close();
      expect(sortRows(await waitForEnd(join2))).toEqual(sortRows([
        { a: '1', b: 'x' },
        { a: '2', b: 'x' },
        { a: '1', b: 'y' },
        { a: '2', b: 'y' },
      ]));
    });

    it('fails when an input fails', async() => {
      const left = openStream();
      const right = openStream();
      const join2 = new SymmetricJoinIterator<IRow>(left, right, () => '', join, false);
      const error = new Error('input failed');
      const failed = new Promise(resolve => join2.on('error', resolve));
      right.emit('error', error);
      await expect(failed).resolves.toBe(error);
      expect(join2.done).toBe(true);
    });

    it('ends without results when one side is empty', async() => {
      const join2 = new SymmetricJoinIterator<IRow>(
        new ArrayIterator<IRow>([{ a: '1' }]),
        new ArrayIterator<IRow>([]),
        () => '',
        join,
        false,
      );
      await expect(waitForEnd(join2)).resolves.toEqual([]);
    });
  });

  describe('as a keyed left join', () => {
    const key = (row: IRow): string => row.r ?? '';

    it('streams matches and emits unmatched left rows when the right side ends', async() => {
      const left = openStream();
      const right = openStream();
      const join2 = new SymmetricJoinIterator<IRow>(left, right, key, join, true);
      left.add({ r: '1', t: 'a' });
      left.add({ r: '2', t: 'b' });
      right.add({ r: '1', s: '5' });
      await expect(settle(join2)).resolves.toEqual([{ r: '1', t: 'a', s: '5' }]);

      right.add({ r: '1', s: '4' });
      right.close();
      expect(sortRows(await settle(join2))).toEqual(sortRows([
        { r: '1', t: 'a', s: '4' },
        { r: '2', t: 'b' },
      ]));

      // Left rows arriving after the right side ended join with what it produced, or stand alone
      left.add({ r: '1', t: 'c' });
      left.add({ r: '3', t: 'd' });
      left.close();
      expect(sortRows(await waitForEnd(join2))).toEqual(sortRows([
        { r: '1', t: 'c', s: '5' },
        { r: '1', t: 'c', s: '4' },
        { r: '3', t: 'd' },
      ]));
    });

    it('emits every left row on its own when the right side is empty', async() => {
      const join2 = new SymmetricJoinIterator<IRow>(
        new ArrayIterator<IRow>([{ r: '1' }, { r: '2' }]),
        new ArrayIterator<IRow>([]),
        key,
        join,
        true,
      );
      expect(sortRows(await waitForEnd(join2))).toEqual(sortRows([{ r: '1' }, { r: '2' }]));
    });

    it('produces every pair once whatever the arrival order', async() => {
      const leftRows = Array.from({ length: 30 }, (_, i) => ({ r: String(i % 4), l: String(i) }));
      const rightRows = Array.from({ length: 20 }, (_, i) => ({ r: String(i % 3), x: String(i) }));
      const expected: IRow[] = [];
      for (const l of leftRows) {
        const matches = rightRows.filter(r => r.r === l.r);
        if (matches.length === 0) {
          expected.push(l);
        }
        for (const r of matches) {
          expected.push({ ...l, ...r });
        }
      }
      const left = openStream();
      const right = openStream();
      const join2 = new SymmetricJoinIterator<IRow>(left, right, key, join, true);
      const results: IRow[] = [];
      for (let i = 0; i < 50; i++) {
        if (i % 5 < 3 && leftRows.length > 0) {
          left.add(leftRows.shift()!);
        } else if (rightRows.length > 0) {
          right.add(rightRows.shift()!);
        }
        await new Promise(resolve => setImmediate(resolve));
        results.push(...drain(join2));
      }
      for (const row of leftRows.splice(0)) {
        left.add(row);
      }
      for (const row of rightRows.splice(0)) {
        right.add(row);
      }
      left.close();
      right.close();
      results.push(...await waitForEnd(join2));
      expect(sortRows(results)).toEqual(sortRows(expected));
    });
  });
});
