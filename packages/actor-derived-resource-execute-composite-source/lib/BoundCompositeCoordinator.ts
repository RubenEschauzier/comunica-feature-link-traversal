import { boundValueKey } from '@comunica/actor-rdf-join-inner-multi-stems';
import type { Bindings } from '@comunica/types';
import type { ILinkTraversalManager } from '@comunica/types-link-traversal';
import type { Algebra } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';

/**
 * How the seed values of a variable come in.
 * - 'one-shot': from a pattern with a constant subject (`<s> p ?x`), or one step on from such a variable. It
 *   describes a single resource, so its few values arrive together, as soon as that resource is read.
 * - 'trickling': from a pattern with a constant object (`?x p <o>`), typically a class, or one step on from such
 *   a variable. Its values come from every source that holds a member, so they keep arriving as traversal finds
 *   those sources.
 */
export type SeedKind = 'one-shot' | 'trickling';

/**
 * Where the seed values of one variable come from: the base operator of a pattern holding a constant, or one
 * step on from a variable that is itself seeded (`from`), along a pattern of the region.
 */
export interface ISeedSource {
  variable: string;
  kind: SeedKind;
  pattern: Algebra.Pattern;
  /**
   * The seeded variable this one is reached from, along the pattern; undefined when the pattern holds the
   * constant itself.
   */
  from?: string;
}

/**
 * A composite block answered by bound requests: the resource is asked for the block only for the seed values
 * of one variable found so far, rather than for all of it.
 */
export interface IBoundTarget {
  /**
   * What the target is, in traces: the resource's domain.
   */
  label: string;
  /**
   * The variable the requests bind.
   */
  variable: RDF.Variable;
  /**
   * Sends one request bound to the given values, resolving with the number of rows once its answer has been
   * read in full.
   */
  send: (values: RDF.Term[]) => Promise<number | void>;
  /**
   * Requests the block whole instead, once bound requests failed; resolves with the number of rows.
   */
  sendWhole?: () => Promise<number | void>;
  /**
   * The values sent so far, keyed by boundValueKey.
   */
  sent: Set<string>;
  /**
   * The values found since the last request was sent, waiting for it to be answered.
   */
  pending: Map<string, RDF.Term>;
  inFlight: boolean;
  /**
   * Set once bound requests failed and the block was requested whole: no further bound requests go out.
   */
  failed: boolean;
}

/**
 * The join controller a coordinator collects seed values from: every tuple its base operators store.
 */
export interface IBaseTupleSource {
  onBaseTuples: (
    select: (operator: { operations: Algebra.Operation[] }) => boolean,
    listener: (item: Bindings) => void,
  ) => void;
}

/**
 * Coordinates the bound composite requests of one query (bind-join-like composites).
 *
 * Seed values come from the constants of the query. For every variable in the region around a constant, the seed
 * plan names the pattern its values come from (see seedPlan), and the base operator of that pattern is tapped:
 * it holds the pattern's tuples from every source, documents and endpoints alike. Composite blocks holding a
 * constant (seeds) are tapped as well, as their rows bypass the base operators.
 *
 * A composite block without a constant that shares a variable with a constant's region is a target: rather than
 * requested whole, it is requested for the seed values of that variable only. Requests go per target (one
 * resource, one block), with at most one in flight: the first is sent as soon as there is a value, and values
 * found while it is answered wait for the next. A request that fails is sent once more; if that fails too, the
 * block is requested whole.
 *
 * Whether a newly found resource is a target or requested whole is decided when it is found. Only one-shot seeds
 * are bound: their values come from the one resource a constant subject names, so they settle at once, and every
 * target is answered in one request. A trickling seed keeps finding values as long as traversal runs, and every
 * value found late costs a request to every target, so its targets are requested whole. A resource is requested
 * whole as well when its variable has no seed source in the query (a target would wait forever), or when the
 * values found so far do not fit in one request (MAX_BATCH): binding then costs a request per batch per target,
 * while the targets bound before keep receiving values.
 *
 * As long as a seed is still read or a request is waiting or underway, traversal is kept from ending, as it would
 * otherwise close the join to composite sources that still have to come.
 */
export class BoundCompositeCoordinator {
  /**
   * The most values one request is sent with, and the most a variable may have when a target is found for it to
   * be bound.
   */
  public static MAX_BATCH = 500;
  /**
   * Whether to write a trace of seed values, decisions and requests to stderr (DERIVED_BOUND_TRACE=1), one
   * `[bound-trace] {json}` line per event, each with the wall time `t` in ms.
   */
  public static TRACE = process.env.DERIVED_BOUND_TRACE === '1';

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

  public static trace(event: Record<string, unknown>): void {
    if (BoundCompositeCoordinator.TRACE) {
      process.stderr.write(`[bound-trace] ${JSON.stringify({ t: Date.now(), ...event })}\n`);
    }
  }

  /**
   * Per variable, the seed values found so far.
   */
  private readonly values = new Map<string, Map<string, RDF.Term>>();
  private readonly targets: IBoundTarget[] = [];
  /**
   * Per variable, how its seed values come in, once a join has been tapped (see tapRegion).
   */
  private plan: Map<string, ISeedSource> | undefined;
  /**
   * Per variable reached one step on from another, the values waiting for their origin to become a seed value:
   * keyed by the origin's boundValueKey.
   */
  private readonly waiting = new Map<string, Map<string, RDF.Term[]>>();
  private seedsOpen = 0;
  private hold: AbortController | undefined;
  private readonly tappedBase = new Set<object>();

  public readonly statistics = {
    seeds: 0,
    targets: 0,
    wholeInstead: 0,
    noSeed: 0,
    trickling: 0,
    tooMany: 0,
    requests: 0,
    valuesSent: 0,
    baseTuples: 0,
    retries: 0,
    fallbacks: 0,
  };

  public constructor(private readonly manager: ILinkTraversalManager) {}

  /**
   * The patterns of the regions around the constants of a join's patterns: the star of every subject a constant
   * pattern has, together with the stars hanging off the object variables of that star.
   */
  public static region(operations: Algebra.Operation[]): Algebra.Pattern[] {
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
    return [ ...star, ...patterns.filter(pattern => !star.includes(pattern) && hanging.has(key(pattern.subject))) ];
  }

  /**
   * The variables of the regions around the constants of a join's patterns, which seeds bind and targets are
   * bound on.
   */
  public static seedableVariables(operations: Algebra.Operation[]): Set<string> {
    const variables = new Set<string>();
    for (const pattern of BoundCompositeCoordinator.region(operations)) {
      for (const term of [ pattern.subject, pattern.object ]) {
        if (term.termType === 'Variable') {
          variables.add(term.value);
        }
      }
    }
    return variables;
  }

  /**
   * Per variable of the region, where its seed values come from. A pattern with a constant subject seeds its
   * object variable one-shot, a pattern with a constant object seeds its subject variable trickling, and a
   * pattern from a seeded subject variable to an object variable seeds that one with the same kind, from the
   * tuples whose subject is a seed value. Any of these sources gives a superset of the values the variable takes
   * in the query's answers, so one suffices: a one-shot source is preferred, as it is the more selective.
   */
  public static seedPlan(operations: Algebra.Operation[]): Map<string, ISeedSource> {
    const region = BoundCompositeCoordinator.region(operations);
    const plan = new Map<string, ISeedSource>();
    const offer = (source: ISeedSource): boolean => {
      const known = plan.get(source.variable);
      if (known === undefined || (known.kind === 'trickling' && source.kind === 'one-shot')) {
        plan.set(source.variable, source);
        return true;
      }
      return false;
    };
    for (const pattern of region) {
      const subjectVariable = pattern.subject.termType === 'Variable';
      const objectVariable = pattern.object.termType === 'Variable';
      if (!subjectVariable && objectVariable) {
        offer({ variable: pattern.object.value, kind: 'one-shot', pattern });
      } else if (subjectVariable && !objectVariable) {
        offer({ variable: pattern.subject.value, kind: 'trickling', pattern });
      }
    }
    let changed = true;
    while (changed) {
      changed = false;
      for (const pattern of region) {
        if (pattern.subject.termType !== 'Variable' || pattern.object.termType !== 'Variable' ||
          pattern.subject.value === pattern.object.value) {
          continue;
        }
        const from = plan.get(pattern.subject.value);
        if (from && from.variable !== pattern.object.value) {
          changed = offer({ variable: pattern.object.value, kind: from.kind, pattern, from: from.variable }) || changed;
        }
      }
    }
    return plan;
  }

  /**
   * Taps the base operators of a join for seed values, following its seed plan (once per join controller): the
   * values its seed sources store so far and from now on.
   */
  public tapRegion(controller: IBaseTupleSource, operations: Algebra.Operation[]): void {
    if (this.tappedBase.has(controller)) {
      return;
    }
    this.tappedBase.add(controller);
    const plan = BoundCompositeCoordinator.seedPlan(operations);
    this.plan = new Map([ ...this.plan ?? [], ...plan ]);
    BoundCompositeCoordinator.trace({ e: 'plan', plan: [ ...plan.values() ].map(source =>
      ({ v: source.variable, kind: source.kind, from: source.from })) });
    const patternKey = (pattern: Algebra.Pattern): string => [ pattern.subject, pattern.predicate, pattern.object ]
      .map(term => `${term.termType}|${term.value}`).join(' ');
    for (const source of plan.values()) {
      const key = patternKey(source.pattern);
      const select = (operator: { operations: Algebra.Operation[] }): boolean => operator.operations.length === 1 &&
        operator.operations[0].type === 'pattern' && patternKey(<Algebra.Pattern> operator.operations[0]) === key;
      const from = source.from;
      controller.onBaseTuples(select, (binding) => {
        this.statistics.baseTuples++;
        const term = binding.get(source.variable);
        if (term === undefined) {
          return;
        }
        if (from === undefined) {
          this.onValue(source.variable, term, source.kind);
          return;
        }
        const origin = binding.get(from);
        if (origin === undefined) {
          return;
        }
        const originKey = boundValueKey(origin);
        if (this.values.get(from)?.has(originKey)) {
          this.onValue(source.variable, term, source.kind);
        } else {
          let waiting = this.waiting.get(source.variable);
          if (waiting === undefined) {
            waiting = new Map();
            this.waiting.set(source.variable, waiting);
          }
          const terms = waiting.get(originKey) ?? [];
          terms.push(term);
          waiting.set(originKey, terms);
        }
      });
    }
  }

  /**
   * Starts collecting the values the seed binds for the given variables, from the rows it gives the join.
   */
  public tapSeed(stream: AsyncIterator<Bindings>, variables: RDF.Variable[], label = ''): {
    stream: AsyncIterator<Bindings>;
    release: () => void;
  } {
    this.seedsOpen++;
    this.statistics.seeds++;
    this.updateHold();
    BoundCompositeCoordinator.trace({ e: 'seed', pod: label, v: variables.map(variable => variable.value) });
    let released = false;
    let rows = 0;
    const release = (): void => {
      if (!released) {
        released = true;
        this.seedsOpen--;
        this.updateHold();
        BoundCompositeCoordinator.trace({ e: 'seed-end', pod: label, rows });
      }
    };
    const tapped = stream.map((binding) => {
      rows++;
      for (const variable of variables) {
        const term = binding.get(variable);
        if (term !== undefined) {
          this.onValue(variable.value, term, 'composite');
        }
      }
      return binding;
    });
    tapped.on('end', release);
    tapped.on('error', release);
    return { stream: tapped, release };
  }

  /**
   * Takes on a newly found resource's block as a target bound on the given variable, unless it is to be requested
   * whole, in which case false is returned: when no seed source in the query gives the variable values, when its
   * seed trickles, or when more values have been found than fit in one request.
   */
  public addTarget(
    variable: RDF.Variable,
    send: (values: RDF.Term[]) => Promise<number | void>,
    options: { label?: string; sendWhole?: () => Promise<number | void> } = {},
  ): boolean {
    const label = options.label ?? '';
    const known = this.values.get(variable.value)?.size ?? 0;
    const whole = (reason: string): false => {
      this.statistics.wholeInstead++;
      BoundCompositeCoordinator.trace({ e: 'target', pod: label, v: variable.value, decision: 'whole', reason, known });
      return false;
    };
    const source = this.plan?.get(variable.value);
    if (this.plan !== undefined && source === undefined && known === 0) {
      this.statistics.noSeed++;
      return whole('no-seed');
    }
    if (source?.kind === 'trickling') {
      this.statistics.trickling++;
      return whole('trickling');
    }
    if (known > BoundCompositeCoordinator.MAX_BATCH) {
      this.statistics.tooMany++;
      return whole('too-many');
    }
    const target: IBoundTarget = {
      label,
      variable,
      send,
      sendWhole: options.sendWhole,
      sent: new Set(),
      pending: new Map(),
      inFlight: false,
      failed: false,
    };
    for (const [ key, term ] of this.values.get(variable.value) ?? []) {
      target.pending.set(key, term);
    }
    this.targets.push(target);
    this.statistics.targets++;
    BoundCompositeCoordinator.trace({
      e: 'target',
      pod: label,
      v: variable.value,
      decision: 'bound',
      kind: source?.kind,
      known,
    });
    this.flush(target);
    return true;
  }

  private onValue(variable: string, term: RDF.Term, from: SeedKind | 'composite'): void {
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
    BoundCompositeCoordinator.trace({ e: 'value', v: variable, from, n: values.size });
    for (const target of this.targets) {
      if (target.variable.value === variable && !target.failed && !target.sent.has(key)) {
        target.pending.set(key, term);
        this.flush(target);
      }
    }
    // The values one step on that waited for this one to become a seed value
    for (const [ next, waiting ] of this.waiting) {
      if (this.plan?.get(next)?.from !== variable) {
        continue;
      }
      const terms = waiting.get(key);
      if (terms !== undefined) {
        waiting.delete(key);
        const kind = this.plan.get(next)!.kind;
        for (const nextTerm of terms) {
          this.onValue(next, nextTerm, kind);
        }
      }
    }
  }

  private flush(target: IBoundTarget): void {
    if (target.inFlight || target.failed || target.pending.size === 0) {
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
    const done = (): void => {
      target.inFlight = false;
      this.flush(target);
      this.updateHold();
    };
    this.sendWithRetry(target, batch).then(done, done);
  }

  /**
   * Sends one bound request, once more if it fails, and requests the block whole if that fails too: the values
   * it was sent with would otherwise get no answer from this resource.
   */
  private async sendWithRetry(target: IBoundTarget, batch: RDF.Term[]): Promise<void> {
    const sentAt = Date.now();
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const rows = await target.send(batch);
        BoundCompositeCoordinator.trace({
          e: 'answer',
          pod: target.label,
          v: target.variable.value,
          values: batch.length,
          rows,
          ms: Date.now() - sentAt,
          attempt,
        });
        return;
      } catch {
        if (attempt === 1) {
          this.statistics.retries++;
        }
      }
    }
    target.failed = true;
    target.pending.clear();
    this.statistics.fallbacks++;
    BoundCompositeCoordinator.trace({ e: 'fallback', pod: target.label, v: target.variable.value });
    if (target.sendWhole) {
      try {
        await target.sendWhole();
      } catch {
        // The base operators keep the solutions a whole request would have given
      }
    }
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
