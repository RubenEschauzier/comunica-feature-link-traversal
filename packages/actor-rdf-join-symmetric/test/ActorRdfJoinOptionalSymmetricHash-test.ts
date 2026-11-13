import { KeysInitQuery } from '@comunica/context-entries';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import { ActorRdfJoinOptionalSymmetricHash } from '../lib/ActorRdfJoinOptionalSymmetricHash';
import { DF, entry, rowsOf, sorted } from './actors-helpers';
import '@comunica/utils-jest';

describe('ActorRdfJoinOptionalSymmetricHash', () => {
  let context: IActionContext;
  let actor: ActorRdfJoinOptionalSymmetricHash;

  beforeEach(() => {
    context = new ActionContext({ [KeysInitQuery.dataFactory.name]: DF });
    actor = new ActorRdfJoinOptionalSymmetricHash({
      name: 'actor',
      bus: new Bus({ name: 'bus' }),
      mediatorJoinSelectivity: <any> { mediate: async() => ({ selectivity: 1 }) },
    });
  });

  describe('test', () => {
    it('rejects an inner join', async() => {
      await expect(actor.test({
        type: 'inner',
        entries: [ entry([], [ 'a' ]), entry([], [ 'a' ]) ],
        context,
      })).resolves.toFailTest(`actor can only handle logical joins of type 'optional', while 'inner' was given.`);
    });

    it('costs a hash join when keyed on a shared variable that is always bound', async() => {
      await expect(actor.test({
        type: 'optional',
        entries: [ entry([], [ 'a', 'b' ], [], 3), entry([], [ 'a', 'c' ], [], 4) ],
        context,
      })).resolves.toPassTest({
        iterations: 7,
        persistedItems: 7,
        blockingItems: 0,
        requestTime: 0,
      });
    });

    it('costs a cartesian product when every shared variable can be unbound', async() => {
      await expect(actor.test({
        type: 'optional',
        entries: [ entry([], [ 'a', 'b' ], [ 'a' ], 3), entry([], [ 'a', 'c' ], [], 4) ],
        context,
      })).resolves.toPassTest({
        iterations: 12,
        persistedItems: 7,
        blockingItems: 0,
        requestTime: 0,
      });
    });
  });

  describe('keyVariables', () => {
    it('keeps the shared variables that are bound on both sides', async() => {
      const metadatas = await Promise.all([
        entry([], [ 'a', 'b', 'u' ], [ 'u' ]),
        entry([], [ 'a', 'b', 'u' ], [ 'b' ]),
      ].map(joinEntry => joinEntry.output.metadata()));
      expect(ActorRdfJoinOptionalSymmetricHash.keyVariables(metadatas)).toEqual([ DF.variable('a') ]);
    });
  });

  describe('run', () => {
    it('joins on the shared variables and keeps unmatched left rows', async() => {
      const result = await actor.run({
        type: 'optional',
        entries: [
          entry([{ a: '1', b: 'x' }, { a: '2', b: 'y' }], [ 'a', 'b' ]),
          entry([{ a: '1', c: 'p' }, { a: '1', c: 'q' }, { a: '3', c: 'r' }], [ 'a', 'c' ]),
        ],
        context,
      }, undefined!);
      expect(rowsOf(await result.bindingsStream.toArray())).toEqual(sorted([
        { a: '1', b: 'x', c: 'p' },
        { a: '1', b: 'x', c: 'q' },
        { a: '2', b: 'y' },
      ]));
      await expect(result.metadata()).resolves.toMatchObject({
        cardinality: { type: 'estimate', value: 6 },
        variables: [
          { variable: DF.variable('a'), canBeUndef: false },
          { variable: DF.variable('b'), canBeUndef: false },
          { variable: DF.variable('c'), canBeUndef: true },
        ],
      });
    });

    it('checks a shared variable that can be unbound when joining, not through the key', async() => {
      const result = await actor.run({
        type: 'optional',
        entries: [
          entry([{ a: '1' }, { b: 'y' }, { a: '2' }], [ 'a', 'b' ], [ 'a', 'b' ]),
          entry([{ a: '1', c: 'p' }, { a: '3', c: 'r' }], [ 'a', 'c' ]),
        ],
        context,
      }, undefined!);
      expect(rowsOf(await result.bindingsStream.toArray())).toEqual(sorted([
        { a: '1', c: 'p' },
        { b: 'y', a: '1', c: 'p' },
        { b: 'y', a: '3', c: 'r' },
        { a: '2' },
      ]));
    });
  });
});
