import { KeysInitQuery } from '@comunica/context-entries';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import { ActorRdfJoinInnerSymmetricCartesian } from '../lib/ActorRdfJoinInnerSymmetricCartesian';
import { DF, entry, rowsOf, sorted } from './actors-helpers';
import '@comunica/utils-jest';

describe('ActorRdfJoinInnerSymmetricCartesian', () => {
  let context: IActionContext;
  let actor: ActorRdfJoinInnerSymmetricCartesian;

  beforeEach(() => {
    context = new ActionContext({ [KeysInitQuery.dataFactory.name]: DF });
    actor = new ActorRdfJoinInnerSymmetricCartesian({
      name: 'actor',
      bus: new Bus({ name: 'bus' }),
      mediatorJoinSelectivity: <any> { mediate: async() => ({ selectivity: 1 }) },
    });
  });

  describe('test', () => {
    it('rejects entries with a shared variable', async() => {
      await expect(actor.test({
        type: 'inner',
        entries: [ entry([], [ 'a', 'b' ]), entry([], [ 'b', 'c' ]) ],
        context,
      })).resolves.toFailTest('actor only joins entries without shared variables');
    });

    it('rejects three entries', async() => {
      await expect(actor.test({
        type: 'inner',
        entries: [ entry([], [ 'a' ]), entry([], [ 'b' ]), entry([], [ 'c' ]) ],
        context,
      })).resolves.toFailTest('actor requires 2 join entries at most. The input contained 3.');
    });

    it('accepts entries without shared variables, at the cost of a cartesian product', async() => {
      await expect(actor.test({
        type: 'inner',
        entries: [ entry([], [ 'a' ], [], 3), entry([], [ 'b' ], [], 4) ],
        context,
      })).resolves.toPassTest({
        iterations: 12,
        persistedItems: 7,
        blockingItems: 0,
        requestTime: 0,
      });
    });
  });

  describe('run', () => {
    it('produces the cartesian product and its metadata', async() => {
      const result = await actor.run({
        type: 'inner',
        entries: [
          entry([{ a: '1' }, { a: '2' }], [ 'a' ]),
          entry([{ b: 'x' }, { b: 'y' }, { b: 'z' }], [ 'b' ]),
        ],
        context,
      }, undefined!);
      expect(rowsOf(await result.bindingsStream.toArray())).toEqual(sorted([
        { a: '1', b: 'x' },
        { a: '1', b: 'y' },
        { a: '1', b: 'z' },
        { a: '2', b: 'x' },
        { a: '2', b: 'y' },
        { a: '2', b: 'z' },
      ]));
      await expect(result.metadata()).resolves.toMatchObject({
        cardinality: { type: 'estimate', value: 6 },
        variables: [
          { variable: DF.variable('a'), canBeUndef: false },
          { variable: DF.variable('b'), canBeUndef: false },
        ],
      });
    });
  });
});
