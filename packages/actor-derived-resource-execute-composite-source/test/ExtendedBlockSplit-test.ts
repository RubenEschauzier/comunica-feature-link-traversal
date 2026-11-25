import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import { ArrayIterator } from 'asynciterator';
import type { AsyncIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ExtendedBlockSplit } from '../lib/ExtendedBlockSplit';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);
const POD = 'http://pod1/';
const v = (name: string): RDF.Variable => DF.variable(name);
const marker = v('hop');

/**
 * A row of the offer star (offer, price, listing), with the listing's product when its hop matched.
 */
function row(offer: string, listing: string, product?: string): any {
  const entries: [RDF.Variable, RDF.Term][] = [
    [ v('offer'), DF.namedNode(offer) ],
    [ v('price'), DF.literal('10') ],
    [ v('listing'), DF.namedNode(listing) ],
  ];
  if (product) {
    entries.push([ v('product'), DF.namedNode(product) ], [ marker, DF.literal('true') ]);
  }
  return BF.bindings(entries);
}

function rows(stream: AsyncIterator<any>): Promise<string[]> {
  return stream.toArray().then(bindings => bindings
    .map(binding => [ ...binding ].map(([ key, value ]) => `${key.value}=${value.value}`).sort().join(' '))
    .sort());
}

describe('ExtendedBlockSplit', () => {
  const split = (withMarker: boolean): ExtendedBlockSplit =>
    new ExtendedBlockSplit([ 'offer', 'price', 'listing' ], [ v('listing') ], POD, withMarker ? marker : undefined);

  it('gives the extended block the rows whose hops are in the pod, and the star every other star row once', async() => {
    const answer = new ArrayIterator<any>([
      // Listing in the pod with its product: the extended block's
      row(`${POD}o1`, `${POD}l1`, 'http://pod9/p1'),
      row(`${POD}o1`, `${POD}l1`, 'http://pod9/p2'),
      // Listing in the pod without hops in the pod: the star's
      row(`${POD}o2`, `${POD}l2`),
      // Listing in another pod, twice as it has two hops there: the star's, once
      row(`${POD}o3`, 'http://pod2/l3', 'http://pod9/p3'),
      row(`${POD}o3`, 'http://pod2/l3', 'http://pod9/p4'),
    ], { autoStart: false });
    const { star, extended } = split(true).splitOptional(answer);
    const [ starRows, extendedRows ] = await Promise.all([ rows(star), rows(extended) ]);
    expect(extendedRows).toEqual([
      `listing=${POD}l1 offer=${POD}o1 price=10 product=http://pod9/p1`,
      `listing=${POD}l1 offer=${POD}o1 price=10 product=http://pod9/p2`,
    ]);
    expect(starRows).toEqual([
      `listing=${POD}l2 offer=${POD}o2 price=10`,
      `listing=http://pod2/l3 offer=${POD}o3 price=10`,
    ]);
  });

  it('holds back the star rows the extended block may extend until its answer is in, for two requests', async() => {
    const starAnswer = new ArrayIterator<any>([
      row(`${POD}o1`, `${POD}l1`),
      row(`${POD}o2`, `${POD}l2`),
      row(`${POD}o3`, 'http://pod2/l3'),
    ], { autoStart: false });
    const extendedAnswer = new ArrayIterator<any>([
      BF.bindings([
        [ v('offer'), DF.namedNode(`${POD}o1`) ],
        [ v('price'), DF.literal('10') ],
        [ v('listing'), DF.namedNode(`${POD}l1`) ],
        [ v('product'), DF.namedNode('http://pod9/p1') ],
      ]),
    ], { autoStart: false });
    const { star, extended } = split(false).splitPair(starAnswer, extendedAnswer);
    const [ starRows, extendedRows ] = await Promise.all([ rows(star), rows(extended) ]);
    expect(extendedRows).toEqual([ `listing=${POD}l1 offer=${POD}o1 price=10 product=http://pod9/p1` ]);
    expect(starRows).toEqual([
      `listing=${POD}l2 offer=${POD}o2 price=10`,
      `listing=http://pod2/l3 offer=${POD}o3 price=10`,
    ]);
  });

  it('passes every star row on when the extended block\'s request fails', async() => {
    const starAnswer = new ArrayIterator<any>([ row(`${POD}o1`, `${POD}l1`) ], { autoStart: false });
    const failing = new ArrayIterator<any>([], { autoStart: false });
    const { star, extended } = split(false).splitPair(starAnswer, failing);
    extended.on('error', () => {
      // The join sees the failure; the star does not depend on it
    });
    failing.emit('error', new Error('endpoint down'));
    await expect(rows(star)).resolves.toEqual([ `listing=${POD}l1 offer=${POD}o1 price=10` ]);
  });
});
