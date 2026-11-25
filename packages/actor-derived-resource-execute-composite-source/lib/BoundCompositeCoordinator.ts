import { boundValueKey } from '@comunica/actor-rdf-join-inner-multi-stems';
import type { Bindings } from '@comunica/types';
import type { ILinkTraversalManager } from '@comunica/types-link-traversal';
import type { Algebra } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';

/**
 * A composite block answered by bound requests: the resource is asked for the block only for the values of
 * one variable that composite blocks with a constant (seeds) have produced so far, rather than for all of it.
 */
export interface IBoundTarget {
  /**
   * The variable the requests bind.
   */
  variable: RDF.Variable;
  /**
   * Sends one request bound to the given values, resolving once its answer has been read in full.
   */
  send: (values: RDF.Term[]) => Promise<void>;
  /**
   * The values sent so far, keyed by boundValueKey.
   */
  sent: Set<string>;
  /**
   * The values found since the last request was sent, waiting for it to be answered.
   */
  pending: Map<string, RDF.Term>;
  inFlight: boolean;
}

/**
 * Estimates, from the join state, how selective binding a variable to the given values is: of the tuples
 * binding it seen so far, how many bind it to one of them.
 */
export type SelectivityEstimate = (variable: string, values: Set<string>) => { matching: number; total: number } | undefined;

/**
 * Coordinates the bound composite requests of one query (bind-join-like composites).
 *
 * Composite blocks holding a constant are seeds: they are requested whole, and the values their rows bind
 * are collected. A composite block without a constant that shares a variable with a seed's region is a
 * target: rather than requested whole, it is requested for the seed values of that variable only, which
 * for a selective constant is a small part of it.
 *
 * Requests go per target (one resource, one block), with at most one in flight: the first is sent as soon
 * as there is a value, and values found while it is answered wait for the next. The size of a batch thus
 * follows how fast the endpoint answers, without a size or timer to tune.
 *
 * Whether a newly found resource is a target or requested whole is decided when it is found: once the seed
 * values turn out to be unselective, every resource found from then on is requested whole, while the targets
 * bound before keep receiving new values.
 *
 * As long as a seed is still read or a request is waiting or underway, traversal is kept from ending, as it
 * would otherwise close the join to composite sources that still have to come.
 */
export class BoundCompositeCoordinator {
  /**
   * The most values one request is sent with.
   */
  public static MAX_BATCH = 500;
  /**
   * Past this share of the tuples binding the variable that bind it to a seed value, binding does not pay.
   */
  public static SELECTIVITY_THRESHOLD = 0.3;
  /**
   * The tuples binding the variable the join has to have seen before the share is trusted.
   */
  public static MIN_EVIDENCE = 200;
  /**
   * Past this many seed values for a variable, binding it does not pay whatever the share.
   */
  public static MAX_VALUES = 5000;

  private static readonly perQuery = new WeakMap<object, BoundCompositeCoordinator>();

  /**
   * The coordinator of the query the given object (its adaptive join controller) is unique to.
   */
  public static forQuery(query: object, manager: ILinkTraversalManager): BoundCompositeCoordinator {
    let coordinator = BoundCompositeCoordinator.perQuery.get(query);
    if (coordinator === undefined) {
      coordinator = new BoundCompositeCoordinator(manager);
      BoundCompositeCoordinator.perQuery.set(query, coordinator);
    }
    return coordinator;
  }

  /**
   * Per variable, the seed values found so far.
   */
  private readonly values = new Map<string, Map<string, RDF.Term>>();
  private readonly targets: IBoundTarget[] = [];
  /**
   * The variables whose seed values turned out unselective.
   */
  private readonly unselective = new Set<string>();
  private seedsOpen = 0;
  private hold: AbortController | undefined;

  public readonly statistics = { seeds: 0, targets: 0, wholeInstead: 0, requests: 0, valuesSent: 0, baseTuples: 0 };

  public constructor(private readonly manager: ILinkTraversalManager) {}

  /**
   * The variables of the regions around the constants of a join's patterns, which seeds bind and targets are
   * bound on: the star of every subject a constant pattern has, together with the stars hanging off the
   * object variables of that star.
   */
  public static seedableVariables(operations: Algebra.Operation[]): Set<string> {
    const patterns = <Algebra.Pattern[]> operations.filter(operation => operation.type === 'pattern');
    const key = (term: RDF.Term): string => `${term.termType}|${term.value}`;
    const constant = (term: RDF.Term): boolean => term.termType !== 'Variable';
    const centres = new Set(patterns
      .filter(pattern => constant(pattern.subject) || constant(pattern.object))
      .map(pattern => key(pattern.subject)));
    const star = patterns.filter(pattern => centres.has(key(pattern.subject)));
    const hanging = new Set(star
      .filter(pattern => pattern.object.termType === 'Variable')
      .map(pattern => key(pattern.object)));
    const region = [ ...star, ...patterns.filter(pattern => hanging.has(key(pattern.subject))) ];
    const variables = new Set<string>();
    for (const pattern of region) {
      for (const term of [ pattern.subject, pattern.object ]) {
        if (term.termType === 'Variable') {
          variables.add(term.value);
        }
      }
    }
    return variables;
  }

  /**
   * The join controller whose base operators are tapped for seed values, once tapped.
   */
  private tappedBase: object | undefined;

  /**
   * Also collects seed values from the base operators: those of a single pattern holding a constant, within
   * the given operations, for the given variables. A seed composite only gives the values of the resources
   * that answer composite blocks; the base operators give the values of every other source, such as plain
   * documents, so that targets are also asked for those.
   */
  public tapBase(
    controller: {
      onBaseTuples: (select: (operator: { operations: Algebra.Operation[] }) => boolean, listener: (item: Bindings) => void) => void;
    },
    operations: Algebra.Operation[],
    variables: Set<string>,
  ): void {
    if (this.tappedBase === controller) {
      return;
    }
    this.tappedBase = controller;
    const key = (pattern: Algebra.Pattern): string => [ pattern.subject, pattern.predicate, pattern.object ]
      .map(term => `${term.termType}|${term.value}`).join(' ');
    const constantPatterns = new Set((<Algebra.Pattern[]> operations.filter(operation => operation.type === 'pattern'))
      .filter(pattern => pattern.subject.termType !== 'Variable' || pattern.object.termType !== 'Variable')
      .map(key));
    controller.onBaseTuples(
      operator => operator.operations.length === 1 && operator.operations[0].type === 'pattern' &&
        constantPatterns.has(key(<Algebra.Pattern> operator.operations[0])),
      (binding) => {
        this.statistics.baseTuples++;
        for (const variable of variables) {
          const term = binding.get(variable);
          if (term !== undefined) {
            this.onValue(variable, term);
          }
        }
      },
    );
  }

  /**
   * Starts collecting the values the seed binds for the given variables, from the rows it gives the join.
   */
  public tapSeed(stream: AsyncIterator<Bindings>, variables: RDF.Variable[]): {
    stream: AsyncIterator<Bindings>;
    release: () => void;
  } {
    this.seedsOpen++;
    this.statistics.seeds++;
    this.updateHold();
    let released = false;
    const release = (): void => {
      if (!released) {
        released = true;
        this.seedsOpen--;
        this.updateHold();
      }
    };
    const tapped = stream.map((binding) => {
      for (const variable of variables) {
        const term = binding.get(variable);
        if (term !== undefined) {
          this.onValue(variable.value, term);
        }
      }
      return binding;
    });
    tapped.on('end', release);
    tapped.on('error', release);
    return { stream: tapped, release };
  }

  /**
   * Takes on a newly found resource's block as a target bound on the given variable, unless the seed values
   * found so far show binding it does not pay, in which case false is returned and the block is to be
   * requested whole.
   */
  public addTarget(
    variable: RDF.Variable,
    send: (values: RDF.Term[]) => Promise<void>,
    estimate: SelectivityEstimate,
  ): boolean {
    if (this.unselective.has(variable.value) || !this.selective(variable.value, estimate)) {
      this.unselective.add(variable.value);
      this.statistics.wholeInstead++;
      return false;
    }
    const target: IBoundTarget = { variable, send, sent: new Set(), pending: new Map(), inFlight: false };
    for (const [ key, term ] of this.values.get(variable.value) ?? []) {
      target.pending.set(key, term);
    }
    this.targets.push(target);
    this.statistics.targets++;
    this.flush(target);
    return true;
  }

  private selective(variable: string, estimate: SelectivityEstimate): boolean {
    const values = this.values.get(variable);
    if (values === undefined || values.size === 0) {
      // A constant is assumed selective until its values show otherwise
      return true;
    }
    if (values.size > BoundCompositeCoordinator.MAX_VALUES) {
      return false;
    }
    const counts = estimate(variable, new Set(values.keys()));
    return counts === undefined || counts.total < BoundCompositeCoordinator.MIN_EVIDENCE ||
      counts.matching / counts.total <= BoundCompositeCoordinator.SELECTIVITY_THRESHOLD;
  }

  private onValue(variable: string, term: RDF.Term): void {
    let values = this.values.get(variable);
    if (values === undefined) {
      values = new Map();
      this.values.set(variable, values);
    }
    const key = boundValueKey(term);
    if (values.has(key)) {
      return;
    }
    values.set(key, term);
    for (const target of this.targets) {
      if (target.variable.value === variable && !target.sent.has(key)) {
        target.pending.set(key, term);
        this.flush(target);
      }
    }
  }

  private flush(target: IBoundTarget): void {
    if (target.inFlight || target.pending.size === 0) {
      return;
    }
    const batch: RDF.Term[] = [];
    for (const [ key, term ] of target.pending) {
      if (batch.length >= BoundCompositeCoordinator.MAX_BATCH) {
        break;
      }
      batch.push(term);
      target.pending.delete(key);
      target.sent.add(key);
    }
    target.inFlight = true;
    this.statistics.requests++;
    this.statistics.valuesSent += batch.length;
    this.updateHold();
    target.send(batch)
      .catch(() => {
        // A failed request has had its answer as much as a succeeding one; the base operators keep the
        // solutions it was to give for values it was not registered for
      })
      .finally(() => {
        target.inFlight = false;
        this.flush(target);
        this.updateHold();
      });
  }

  /**
   * Keeps traversal from ending while a seed is read or a request waits or is underway.
   */
  private updateHold(): void {
    const busy = this.seedsOpen > 0 || this.targets.some(target => target.inFlight || target.pending.size > 0);
    if (busy && this.hold === undefined) {
      this.hold = new AbortController();
      this.manager.addDereferencingDerivedResource(this.hold);
    } else if (!busy && this.hold !== undefined) {
      const hold = this.hold;
      this.hold = undefined;
      this.manager.completeDereferencingDerivedResource(hold);
    }
  }
}
