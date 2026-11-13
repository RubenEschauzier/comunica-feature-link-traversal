import type { IJoinEntry } from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';

export const DF = new DataFactory();
export const BF = new BindingsFactory(DF);

/**
 * A join entry over the given rows, where every row maps variable names to literal values, declaring the
 * given variables and which of them can be unbound.
 */
export function entry(
  rows: Record<string, string>[],
  variables: string[],
  undefs: string[] = [],
  cardinality = rows.length,
): IJoinEntry {
  return <any> {
    output: {
      type: 'bindings',
      bindingsStream: new ArrayIterator(rows.map(row =>
        BF.bindings(Object.entries(row).map(([ key, value ]) => [ DF.variable(key), DF.literal(value) ]))), {
        autoStart: false,
      }),
      metadata: async() => ({
        state: new MetadataValidationState(),
        cardinality: { type: 'estimate', value: cardinality },
        variables: variables.map(name => ({ variable: DF.variable(name), canBeUndef: undefs.includes(name) })),
      }),
    },
    operation: <any> {},
  };
}

function byString(left: string, right: string): number {
  return left.localeCompare(right);
}

function byKey(left: [string, string], right: [string, string]): number {
  return byString(left[0], right[0]);
}

/**
 * The rows of a bindings array, as sorted strings, to compare independent of order.
 */
export function rowsOf(bindings: any[]): string[] {
  return bindings.map(binding => JSON.stringify(Object.fromEntries(
    [ ...binding ].map(([ key, value ]: any): [string, string] => [ key.value, value.value ]).sort(byKey),
  ))).sort(byString);
}

export function sorted(rows: Record<string, string>[]): string[] {
  return rows.map(row => JSON.stringify(Object.fromEntries(Object.entries(row).sort(byKey)))).sort(byString);
}
