import { Algebra, AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { UnionPushdown } from '../lib/UnionPushdown';

const DF = new DataFactory();
const AF = new AlgebraFactory();
const v = (name: string) => DF.variable(name);
const n = (name: string) => DF.namedNode(`http://ex.org/${name}`);
const p = (s: any, pr: string, o: any): Algebra.Pattern => AF.createPattern(s, n(pr), o, DF.defaultGraph());

describe('UnionPushdown', () => {
  const pushdown = new UnionPushdown(AF);

  it('moves an isolated pattern into the branches it forms a star in', () => {
    const date = p(v('k'), 'creationDate', v('d'));
    const names = [ p(v('p'), 'firstName', v('f')), p(v('p'), 'lastName', v('l')) ];
    const join = AF.createJoin([
      AF.createUnion([
        AF.createBgp([ p(n('me'), 'knows', v('k')), p(v('k'), 'hasPerson', v('p')) ]),
        AF.createBgp([ p(v('p'), 'knows', v('k')), p(v('k'), 'hasPerson', n('me')) ]),
      ]),
      AF.createBgp([ date, ...names ]),
    ], false);

    const result = <Algebra.Join> pushdown.rewrite(join);
    const union = <Algebra.Union> result.input.find(input => input.type === Algebra.Types.UNION);
    for (const branch of union.input) {
      expect((<Algebra.Bgp> branch).patterns).toEqual(expect.arrayContaining([ date ]));
    }
    // The star outside stays where it is
    const outside = <Algebra.Bgp> result.input.find(input => input.type === Algebra.Types.BGP);
    expect(outside.patterns).toEqual(names);
  });

  it('gives every branch a pattern object of its own', () => {
    const join = AF.createJoin([
      AF.createUnion([
        AF.createBgp([ p(v('k'), 'hasPerson', v('p')) ]),
        AF.createBgp([ p(v('k'), 'hasPerson', n('me')) ]),
      ]),
      AF.createBgp([ p(v('k'), 'creationDate', v('d')) ]),
    ], false);

    const result = <Algebra.Union> pushdown.rewrite(join);
    const [ left, right ] = result.input.map(branch => (<Algebra.Bgp> branch).patterns
      .find(pattern => pattern.predicate.value.endsWith('creationDate'))!);
    expect(left).toEqual(right);
    expect(left).not.toBe(right);
  });

  it('moves an outer star into the branches whose only pattern it gives a partner', () => {
    const star = [ p(v('m'), 'hasCreator', n('me')), p(v('m'), 'content', v('c')) ];
    const join = AF.createJoin([
      AF.createBgp(star),
      AF.createUnion([
        AF.createBgp([ p(v('m'), 'type', n('Post')) ]),
        AF.createBgp([ p(v('m'), 'type', n('Comment')) ]),
      ]),
    ], false);

    const result = pushdown.rewrite(join);
    expect(result.type).toBe(Algebra.Types.UNION);
    for (const branch of (<Algebra.Union> result).input) {
      expect((<Algebra.Bgp> branch).patterns).toEqual(expect.arrayContaining(star));
    }
  });

  it('leaves a join alone when moving would not leave fewer isolated patterns', () => {
    const join = AF.createJoin([
      AF.createBgp([ p(v('p'), 'firstName', v('f')), p(v('p'), 'lastName', v('l')) ]),
      AF.createUnion([
        AF.createBgp([ p(v('x'), 'a', v('y')), p(v('x'), 'b', v('z')) ]),
        AF.createBgp([ p(v('x'), 'c', v('y')), p(v('x'), 'd', v('z')) ]),
      ]),
    ], false);

    expect(pushdown.rewrite(join)).toEqual(join);
  });

  it('counts patterns that no other pattern forms a star or chain with', () => {
    expect(pushdown.isolated([ p(v('a'), 'x', v('b')), p(v('b'), 'y', v('c')), p(v('d'), 'z', v('e')) ])).toBe(1);
    // A pattern with a constant subject can be requested selectively, so it is never isolated
    expect(pushdown.isolated([ p(n('me'), 'x', v('b')) ])).toBe(0);
  });
});
