import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type { AsyncIterator } from 'asynciterator';
import { BufferedIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { BoundCompositeCoordinator } from '../lib/BoundCompositeCoordinator';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);
const AF = new AlgebraFactory(DF);
const product = DF.variable('product');

function manager(): any {
  return {
    held: 0,
    addDereferencingDerivedResource() {
      this.held++;
    },
    completeDereferencingDerivedResource() {
      this.held--;
    },
  };
}

/**
 * A seed stream whose rows are pushed by the test, as a composite answer arrives.
 */
function seed(): { stream: BufferedIterator<any>; push: (value: string) => void; end: () => void } {
  const stream = new BufferedIterator<any>({ autoStart: false });
  return {
    stream,
    push: value => (<any> stream)._push(BF.bindings([[ product, DF.namedNode(value) ]])),
    end: () => stream.close(),
  };
}

function drain(stream: AsyncIterator<any>): void {
  stream.on('data', () => {
    // Read it, as the join does
  });
}

/**
 * A target whose requests are answered when the test says so.
 */
function target(): { sent: string[][]; answer: () => void; send: (values: any[]) => Promise<void> } {
  const sent: string[][] = [];
  const answers: (() => void)[] = [];
  return {
    sent,
    answer: () => answers.shift()!(),
    send: values => new Promise<void>((resolve) => {
      sent.push(values.map(value => value.value));
      answers.push(resolve);
    }),
  };
}

const tick = (): Promise<void> => new Promise(resolve => setImmediate(resolve));
const noEstimate = (): undefined => undefined;

describe('BoundCompositeCoordinator', () => {
  afterEach(() => {
    BoundCompositeCoordinator.MIN_EVIDENCE = 200;
  });

  it('keeps one request in flight per target, batching what comes in meanwhile', async() => {
    const m = manager();
    const coordinator = new BoundCompositeCoordinator(m);
    const s = seed();
    const tapped = coordinator.tapSeed(s.stream, [ product ]);
    drain(tapped.stream);
    const t = target();
    expect(coordinator.addTarget(product, t.send, noEstimate)).toBe(true);

    s.push('http://p/1');
    await tick();
    expect(t.sent).toEqual([[ 'http://p/1' ]]);
    s.push('http://p/2');
    s.push('http://p/3');
    s.push('http://p/1');
    await tick();
    // The first request is still underway, so the new values wait, and a value already sent is not again
    expect(t.sent).toEqual([[ 'http://p/1' ]]);
    t.answer();
    await tick();
    expect(t.sent).toEqual([[ 'http://p/1' ], [ 'http://p/2', 'http://p/3' ]]);
    expect(m.held).toBe(1);
    s.end();
    await tick();
    expect(m.held).toBe(1);
    t.answer();
    await tick();
    await tick();
    // Nothing left to read or send: traversal may end
    expect(m.held).toBe(0);
  });

  it('sends a target found later all values found so far in one request', async() => {
    const coordinator = new BoundCompositeCoordinator(manager());
    const s = seed();
    drain(coordinator.tapSeed(s.stream, [ product ]).stream);
    s.push('http://p/1');
    s.push('http://p/2');
    await tick();
    const t = target();
    coordinator.addTarget(product, t.send, noEstimate);
    expect(t.sent).toEqual([[ 'http://p/1', 'http://p/2' ]]);
  });

  it('has resources found once the values are unselective requested whole, and keeps the targets bound so far', async() => {
    BoundCompositeCoordinator.MIN_EVIDENCE = 10;
    const coordinator = new BoundCompositeCoordinator(manager());
    const s = seed();
    drain(coordinator.tapSeed(s.stream, [ product ]).stream);
    s.push('http://p/1');
    await tick();
    const early = target();
    expect(coordinator.addTarget(product, early.send, () => ({ matching: 1, total: 100 }))).toBe(true);
    const late = target();
    expect(coordinator.addTarget(product, late.send, () => ({ matching: 60, total: 100 }))).toBe(false);
    // Once found unselective, without asking again
    expect(coordinator.addTarget(product, target().send, () => ({ matching: 0, total: 100 }))).toBe(false);
    early.answer();
    s.push('http://p/2');
    await tick();
    expect(early.sent).toEqual([[ 'http://p/1' ], [ 'http://p/2' ]]);
    expect(coordinator.statistics.wholeInstead).toBe(2);
  });

  it('trusts the estimate only with enough evidence', async() => {
    const coordinator = new BoundCompositeCoordinator(manager());
    const s = seed();
    drain(coordinator.tapSeed(s.stream, [ product ]).stream);
    s.push('http://p/1');
    await tick();
    expect(coordinator.addTarget(product, target().send, () => ({ matching: 9, total: 10 }))).toBe(true);
  });

  it('finds the variables around the constants of a join', () => {
    const p = (s: any, pr: string, o: any): any => AF.createPattern(s, DF.namedNode(pr), o, DF.defaultGraph());
    const v = (name: string): any => DF.variable(name);
    const operations = [
      p(v('product'), 'type', DF.namedNode('http://t/T')),
      p(v('product'), 'producer', v('brand')),
      p(v('brand'), 'label', v('brandLabel')),
      p(v('listing'), 'listingOf', v('product')),
      p(v('offer'), 'product', v('listing')),
    ];
    expect([ ...BoundCompositeCoordinator.seedableVariables(operations) ].sort())
      .toEqual([ 'brand', 'brandLabel', 'product' ]);
  });

  it('also binds targets to the values that base operators holding a constant store, before and after tapping', async() => {
    const coordinator = new BoundCompositeCoordinator(manager());
    const typePattern = AF.createPattern(product, DF.namedNode('type'), DF.namedNode('T'), DF.defaultGraph());
    const producerPattern = AF.createPattern(product, DF.namedNode('producer'), DF.variable('brand'), DF.defaultGraph());
    const row = (value: string): any => BF.bindings([[ product, DF.namedNode(value) ]]);
    const typeOperator = { operations: [ typePattern ], stored: [ row('p1') ], listeners: <any[]> []};
    const producerOperator = { operations: [ producerPattern ], stored: [ row('p9') ], listeners: <any[]> []};
    const controller = {
      onBaseTuples(select: (operator: any) => boolean, listener: (item: any) => void) {
        for (const operator of [ typeOperator, producerOperator ]) {
          if (select(operator)) {
            operator.stored.forEach(listener);
            operator.listeners.push(listener);
          }
        }
      },
    };
    coordinator.tapBase(controller, [ typePattern, producerPattern ], new Set([ 'product' ]));
    coordinator.tapBase(controller, [ typePattern, producerPattern ], new Set([ 'product' ]));
    expect(typeOperator.listeners).toHaveLength(1);
    expect(producerOperator.listeners).toHaveLength(0);

    const t = target();
    expect(coordinator.addTarget(product, t.send, noEstimate)).toBe(true);
    expect(t.sent).toEqual([[ 'p1' ]]);
    typeOperator.listeners[0](row('p2'));
    t.answer();
    await tick();
    expect(t.sent).toEqual([[ 'p1' ], [ 'p2' ]]);
    expect(coordinator.statistics.baseTuples).toBe(2);
  });
});
