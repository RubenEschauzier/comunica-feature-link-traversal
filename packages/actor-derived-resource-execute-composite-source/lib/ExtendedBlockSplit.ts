import { boundValueKey, isDomainDelimited, isWithinDomain } from '@comunica/actor-rdf-join-inner-multi-stems';
import type { Bindings } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator, TransformIterator, UnionIterator } from 'asynciterator';

/**
 * Splits the answer of a pod for an extended block (a star together with the stars hanging off its object
 * variables, the hubs) over two composite sources: the extended block, and the star on its own.
 *
 * A star row goes to the extended block when its hops are answered within the pod: every hub is in the pod and
 * the pod has the hops' triples for it. As the pod holds every triple whose subject it hosts, it then answers
 * the extended block in full for that row, so the row needs no join with the hops on the client. Every other
 * star row goes to the star: a hub outside the pod, whose hops other pods hold, or a hub without hops in the
 * pod. Each star row is thus held by exactly one of the two, so both can be taken on by the join: the star with
 * its claim on the pod (and the store-level drop of its patterns), the extended block with its claim on the pod
 * for the star and the hubs. A hop answered within the pod is then never joined again with the star row.
 *
 * The two have to be taken on together, as neither holds all star rows: both are blocks of the same join
 * component, added one after the other, so the join takes on both or neither.
 */
export class ExtendedBlockSplit {
  private readonly coreVariables: Set<string>;
  private readonly domainIsDelimited: boolean;

  /**
   * @param coreVariables The variables of the star.
   * @param hubs The object variables of the star the hops hang off.
   * @param domain The domain of the pod.
   * @param marker For an OPTIONAL answer, the variable bound within the OPTIONAL, which tells whether the hops
   * matched also when they bind no variable of their own.
   */
  public constructor(
    coreVariables: Iterable<string>,
    private readonly hubs: RDF.Variable[],
    private readonly domain: string,
    private readonly marker?: RDF.Variable,
  ) {
    this.coreVariables = new Set(coreVariables);
    this.domainIsDelimited = isDomainDelimited(domain);
  }

  /**
   * Whether every hub of the row is in the pod.
   */
  public hubsInPod(binding: Bindings): boolean {
    return this.hubs.every((hub) => {
      const term = binding.get(hub);
      return term !== undefined &&
        (term.termType === 'BlankNode' || isWithinDomain(term.value, this.domain, this.domainIsDelimited));
    });
  }

  /**
   * The star row a row holds, as a key.
   */
  public key(binding: Bindings): string {
    return [ ...this.coreVariables ].map((variable) => {
      const term = binding.get(variable);
      return term === undefined ? '' : boundValueKey(term);
    }).join(' ');
  }

  /**
   * The star row a row holds.
   */
  public project(binding: Bindings): Bindings {
    return binding.filter((_, variable) => this.coreVariables.has(variable.value));
  }

  /**
   * Splits the answer to `star OPTIONAL { hops BIND(true AS marker) }`. Its rows hold a star row with its hops,
   * or a star row alone when the pod holds no hops for it.
   */
  public splitOptional(rows: AsyncIterator<Bindings>): {
    star: AsyncIterator<Bindings>;
    extended: AsyncIterator<Bindings>;
  } {
    const extendsStar = (binding: Bindings): boolean =>
      (this.marker === undefined || binding.has(this.marker)) && this.hubsInPod(binding);
    const extended = rows.clone()
      .filter(extendsStar)
      .map(binding => this.marker === undefined ? binding : binding.delete(this.marker));
    const seen = new Set<string>();
    const star = rows.clone()
      .filter(binding => !extendsStar(binding))
      .map(binding => this.project(binding))
      .filter((binding) => {
        // A star row with hops leaving the pod comes once per hop
        const key = this.key(binding);
        if (seen.has(key)) {
          return false;
        }
        seen.add(key);
        return true;
      });
    return { star, extended };
  }

  /**
   * Splits the answers to two requests, one for the star and one for the extended block. A star row whose hubs
   * are all in the pod is held back until the extended block's answer is in, and passed on only when that has no
   * row for it; the others are passed on right away.
   */
  public splitPair(
    starRows: AsyncIterator<Bindings>,
    extendedRows: AsyncIterator<Bindings>,
  ): { star: AsyncIterator<Bindings>; extended: AsyncIterator<Bindings> } {
    const extended = extendedRows.clone();
    const keysClone = extendedRows.clone();
    const keys = new Promise<Set<string>>((resolve) => {
      const found = new Set<string>();
      keysClone.on('data', (binding: Bindings) => found.add(this.key(binding)));
      keysClone.on('end', () => resolve(found));
      // Without the extended block's answer, the star passes on every row, as nothing else answers them
      keysClone.on('error', () => resolve(new Set()));
    });
    const now = starRows.clone().filter(binding => !this.hubsInPod(binding));
    const heldClone = starRows.clone().filter(binding => this.hubsInPod(binding));
    const held = new TransformIterator<Bindings>(Promise.all([ keys, heldClone.toArray() ])
      .then(([ found, bindings ]) => new ArrayIterator(
        bindings.filter(binding => !found.has(this.key(binding))),
        { autoStart: false },
      )), { autoStart: false });
    return { star: new UnionIterator([ now, held ], { autoStart: false }), extended };
  }
}
