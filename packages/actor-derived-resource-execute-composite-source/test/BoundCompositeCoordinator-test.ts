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

describe('BoundCompositeCoordinator', () => {
  it('keeps one request in flight per target, batching what comes in meanwhile', async() => {
    const m = manager();
    const coordinator = new BoundCompositeCoordinator(m);
    const s = seed();
    const tapped = coordinator.tapSeed(s.stream, [ product ]);
    drain(tapped.stream);
    const t = target();
    expect(coordinator.addTarget(product, t.send)).toBe(true);

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
    coordinator.addTarget(product, t.send);
    expect(t.sent).toEqual([[ 'http://p/1', 'http://p/2' ]]);
  });

  it('requests targets whole once more values are known than fit in one request, keeping earlier ones', async() => {
    const max = BoundCompositeCoordinator.MAX_BATCH;
    BoundCompositeCoordinator.MAX_BATCH = 2;
    try {
      const coordinator = new BoundCompositeCoordinator(manager());
      const s = seed();
      drain(coordinator.tapSeed(s.stream, [ product ]).stream);
      s.push('http://p/1');
      s.push('http://p/2');
      await tick();
      const early = target();
      expect(coordinator.addTarget(product, early.send)).toBe(true);
      s.push('http://p/3');
      await tick();
      expect(coordinator.addTarget(product, target().send)).toBe(false);
      expect(coordinator.statistics.tooMany).toBe(1);
      early.answer();
      await tick();
      expect(early.sent).toEqual([[ 'http://p/1', 'http://p/2' ], [ 'http://p/3' ]]);
    } finally {
      BoundCompositeCoordinator.MAX_BATCH = max;
    }
  });

  it('finds the variables around the constants of a join', () => {
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

  const p = (s: any, pr: string, o: any): any => AF.createPattern(s, DF.namedNode(pr), o, DF.defaultGraph());
  const v = (name: string): any => DF.variable(name);
  const iri = (value: string): any => DF.namedNode(value);

  /**
   * A join controller whose base operators are given per pattern, with the tuples they hold already.
   */
  function controllerOf(operators: { operations: any[]; stored: any[]; listeners: any[] }[]): any {
    return {
      onBaseTuples(select: (operator: any) => boolean, listener: (item: any) => void) {
        for (const operator of operators) {
          if (select(operator)) {
            operator.stored.forEach(listener);
            operator.listeners.push(listener);
          }
        }
      },
    };
  }

  it('plans one-shot seeds from constant subjects and trickling ones from constant objects, one step on as well', () => {
    const plan = BoundCompositeCoordinator.seedPlan([
      p(iri('http://o/Offer1'), 'product', v('listing')),
      p(v('listing'), 'listingOf', v('product')),
      p(v('product'), 'type', iri('http://t/T')),
      p(v('product'), 'label', v('label')),
    ]);
    expect(plan.get('listing')).toMatchObject({ kind: 'one-shot' });
    expect(plan.get('listing')!.from).toBeUndefined();
    // Reached one step on from a one-shot seed, which wins over the trickling type pattern
    expect(plan.get('product')).toMatchObject({ kind: 'one-shot', from: 'listing' });
    expect(plan.get('label')).toMatchObject({ kind: 'one-shot', from: 'product' });

    const classOnly = BoundCompositeCoordinator.seedPlan([
      p(v('product'), 'type', iri('http://t/T')),
      p(v('product'), 'producer', v('brand')),
    ]);
    expect(classOnly.get('product')).toMatchObject({ kind: 'trickling' });
    expect(classOnly.get('product')!.from).toBeUndefined();
    expect(classOnly.get('brand')).toMatchObject({ kind: 'trickling', from: 'product' });
  });

  it('binds targets to the values the base operators of the seed plan store, before and after tapping', async() => {
    const coordinator = new BoundCompositeCoordinator(manager());
    const sellsPattern = p(iri('http://s/Shop'), 'sells', product);
    const producerPattern = p(product, 'producer', v('brand'));
    const row = (value: string): any => BF.bindings([[ product, iri(value) ]]);
    const sellsOperator = { operations: [ sellsPattern ], stored: [ row('p1') ], listeners: <any[]> []};
    const controller = controllerOf([ sellsOperator ]);
    coordinator.tapRegion(controller, [ sellsPattern, producerPattern ]);
    coordinator.tapRegion(controller, [ sellsPattern, producerPattern ]);
    expect(sellsOperator.listeners).toHaveLength(1);

    const t = target();
    expect(coordinator.addTarget(product, t.send)).toBe(true);
    expect(t.sent).toEqual([[ 'p1' ]]);
    sellsOperator.listeners[0](row('p2'));
    t.answer();
    await tick();
    expect(t.sent).toEqual([[ 'p1' ], [ 'p2' ]]);
  });

  it('passes values on one step only from tuples whose origin is a seed value, also when the origin comes later', () => {
    const coordinator = new BoundCompositeCoordinator(manager());
    const offerPattern = p(iri('http://o/Offer1'), 'product', v('listing'));
    const listingPattern = p(v('listing'), 'listingOf', product);
    const listingRow = (listing: string, value: string): any =>
      BF.bindings([[ v('listing'), iri(listing) ], [ product, iri(value) ]]);
    const offerOperator = { operations: [ offerPattern ], stored: <any[]> [], listeners: <any[]> []};
    const listingOperator = {
      operations: [ listingPattern ],
      stored: [ listingRow('l1', 'p1'), listingRow('l2', 'p2') ],
      listeners: <any[]> [],
    };
    coordinator.tapRegion(controllerOf([ offerOperator, listingOperator ]), [ offerPattern, listingPattern ]);
    const t = target();
    expect(coordinator.addTarget(product, t.send)).toBe(true);
    expect(t.sent).toEqual([]);

    // l1 becomes a seed value after its listing tuple was stored, so p1 follows, while p2 never does
    offerOperator.listeners[0](BF.bindings([[ v('listing'), iri('l1') ]]));
    expect(t.sent).toEqual([[ 'p1' ]]);
  });

  it('requests a target whole when no seed source gives its variable values', () => {
    const coordinator = new BoundCompositeCoordinator(manager());
    const typePattern = p(product, 'type', iri('T'));
    coordinator.tapRegion(controllerOf([]), [ typePattern ]);
    expect(coordinator.addTarget(v('vendor'), target().send)).toBe(false);
    expect(coordinator.statistics.noSeed).toBe(1);
  });

  it('requests targets with a trickling seed whole, and binds those with a one-shot seed', () => {
    const coordinator = new BoundCompositeCoordinator(manager());
    coordinator.tapRegion(controllerOf([]), [
      p(product, 'type', iri('T')),
      p(iri('http://p/Product1'), 'producer', v('brand')),
    ]);
    expect(coordinator.addTarget(product, target().send)).toBe(false);
    expect(coordinator.addTarget(v('brand'), target().send)).toBe(true);
    expect(coordinator.statistics.trickling).toBe(1);
  });

  it('retries a failed bound request once, then requests the block whole and sends no more bound requests', async() => {
    const m = manager();
    const coordinator = new BoundCompositeCoordinator(m);
    const s = seed();
    drain(coordinator.tapSeed(s.stream, [ product ]).stream);
    let attempts = 0;
    let wholeRequests = 0;
    const send = (): Promise<number> => {
      attempts++;
      return Promise.reject(new Error('endpoint down'));
    };
    const sendWhole = (): Promise<number> => {
      wholeRequests++;
      return Promise.resolve(3);
    };
    expect(coordinator.addTarget(product, send, { sendWhole })).toBe(true);
    s.push('http://p/1');
    await tick();
    await tick();
    expect(attempts).toBe(2);
    expect(wholeRequests).toBe(1);
    expect(coordinator.statistics).toMatchObject({ retries: 1, fallbacks: 1 });
    s.push('http://p/2');
    await tick();
    expect(attempts).toBe(2);
    s.end();
    await tick();
    expect(m.held).toBe(0);
  });
});
