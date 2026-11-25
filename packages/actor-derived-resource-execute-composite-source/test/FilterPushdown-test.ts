import { AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { FilterPushdown } from '../lib/FilterPushdown';

const DF = new DataFactory();
const AF = new AlgebraFactory(DF);
const v = (name: string): any => DF.variable(name);
const n = (iri: string): any => DF.namedNode(iri);
const p = (s: any, pr: string, o: any): any => AF.createPattern(s, n(pr), o, DF.defaultGraph());
const op = (operator: string, ...args: any[]): any => AF.createOperatorExpression(operator, args);
const t = (term: any): any => AF.createTermExpression(term);

// The review star and the product star of q16
const reviewFor = p(v('review'), 'reviewFor', v('page'));
const rating = p(v('review'), 'rating1', v('rating1'));
const publisher = p(v('review'), 'publisher', v('site'));
const type = p(v('product'), 'type', n('http://t/T'));
const producer = p(v('product'), 'producer', v('brand'));
const ratingFilter = op('>=', t(v('rating1')), t(DF.literal('8')));
const productFilter = op('!=', t(v('product')), t(n('http://p/1')));

describe('FilterPushdown', () => {
  it('pushes a filter over the block\'s variables into it, and not one over other variables', () => {
    const query = AF.createProject(AF.createFilter(
      AF.createBgp([ type, producer, reviewFor, rating, publisher ]),
      ratingFilter,
    ), [ v('product') ]);
    expect(FilterPushdown.pushableFilters(query, [ reviewFor, rating, publisher ])).toEqual([ ratingFilter ]);
    expect(FilterPushdown.pushableFilters(query, [ type, producer ])).toEqual([]);
  });

  it('splits conjunctions, giving each block the conjuncts over its own variables', () => {
    const query = AF.createFilter(
      AF.createBgp([ type, producer, reviewFor, rating, publisher ]),
      op('&&', productFilter, ratingFilter),
    );
    expect(FilterPushdown.pushableFilters(query, [ reviewFor, rating, publisher ])).toEqual([ ratingFilter ]);
    expect(FilterPushdown.pushableFilters(query, [ type, producer ])).toEqual([ productFilter ]);
  });

  it('collects nested filters', () => {
    const query = AF.createFilter(AF.createFilter(AF.createBgp([ type, producer ]), productFilter), op('bound', t(v('brand'))));
    expect(FilterPushdown.pushableFilters(query, [ type, producer ])).toHaveLength(2);
  });

  it('does not push a filter into the optional side of an OPTIONAL, but does into the required side', () => {
    const query = AF.createFilter(
      AF.createLeftJoin(AF.createBgp([ type, producer ]), AF.createBgp([ reviewFor, rating, publisher ])),
      op('&&', productFilter, ratingFilter),
    );
    expect(FilterPushdown.pushableFilters(query, [ reviewFor, rating, publisher ])).toEqual([]);
    expect(FilterPushdown.pushableFilters(query, [ type, producer ])).toEqual([ productFilter ]);
  });

  it('does not push a filter that misses one occurrence of a block pattern', () => {
    const query = AF.createUnion([
      AF.createFilter(AF.createBgp([ type, producer ]), productFilter),
      AF.createBgp([ type, producer ]),
    ]);
    expect(FilterPushdown.pushableFilters(query, [ type, producer ])).toEqual([]);
  });

  it('pushes a filter over a union into the blocks of each branch', () => {
    const query = AF.createFilter(AF.createUnion([
      AF.createBgp([ type, producer ]),
      AF.createBgp([ reviewFor, rating, publisher ]),
    ]), productFilter);
    expect(FilterPushdown.pushableFilters(query, [ type, producer ])).toEqual([ productFilter ]);
  });

  it('does not push a filter across a LIMIT', () => {
    const query = AF.createFilter(AF.createSlice(AF.createBgp([ type, producer ]), 0, 10), productFilter);
    expect(FilterPushdown.pushableFilters(query, [ type, producer ])).toEqual([]);
  });

  it('does not push EXISTS, or a filter without variables', () => {
    const exists = AF.createExistenceExpression(false, AF.createBgp([ rating ]));
    const constant = op('=', t(DF.literal('1')), t(DF.literal('1')));
    const query = AF.createFilter(AF.createFilter(AF.createBgp([ type, producer ]), exists), constant);
    expect(FilterPushdown.pushableFilters(query, [ type, producer ])).toEqual([]);
  });

  it('pushes nothing for a block pattern not in the query', () => {
    const query = AF.createFilter(AF.createBgp([ type ]), productFilter);
    expect(FilterPushdown.pushableFilters(query, [ type, producer ])).toEqual([]);
  });
});
