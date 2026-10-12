import type { AlgebraFactory } from '@comunica/utils-algebra';
import { Algebra, algebraUtils } from '@comunica/utils-algebra';

/**
 * Moves triple patterns joined with a UNION into each of its branches, where that lets them be
 * delegated to a composite source rather than requested pod-wide.
 *
 * A composite answers a star (patterns sharing their subject) or a chain of two patterns, and only
 * within one join: a pattern beside a UNION cannot form one with the patterns inside it. Joining
 * distributes over a union, P ⋈ (B₁ ∪ … ∪ Bₙ) = (P ⋈ B₁) ∪ … ∪ (P ⋈ Bₙ), for the multisets SPARQL
 * evaluates to, so a pattern can always be moved into every branch without changing the answers.
 *
 * Moving is not free, as the moved patterns are then evaluated once per branch, so it is only done when
 * it lowers the number of isolated patterns: those with a variable subject that nothing in their join
 * forms a star or chain with. Those are the patterns a derived resource can only answer for a whole pod.
 */
export class UnionPushdown {
  public constructor(protected readonly algebraFactory: AlgebraFactory) {}

  /**
   * The operation with patterns moved into the unions they are joined with, wherever that helps.
   */
  public rewrite(operation: Algebra.Operation): Algebra.Operation {
    return algebraUtils.mapOperation(operation, {
      [Algebra.Types.JOIN]: {
        transform: (join: Algebra.Join) => this.distribute(join),
      },
    });
  }

  /**
   * Moves patterns of the join into the first union among its inputs that they help in.
   */
  public distribute(join: Algebra.Join): Algebra.Operation {
    const unionIndex = join.input.findIndex(input => input.type === Algebra.Types.UNION);
    if (unionIndex < 0) {
      return join;
    }
    const union = <Algebra.Union> join.input[unionIndex];
    const others = join.input.filter((_, index) => index !== unionIndex);

    // Only patterns standing directly in the join are moved, as part of a BGP or on their own
    const outer = others.flatMap(input => this.patternsOf(input) ?? []);
    const branches = union.input.map(branch => this.patternsOf(branch) ?? []);

    const moved = this.patternsToMove(outer, branches);
    const before = this.isolated(outer) + branches.reduce((sum, branch) => sum + this.isolated(branch), 0);
    const remaining = outer.filter(pattern => !moved.includes(pattern));
    const after = this.isolated(remaining) +
      branches.reduce((sum, branch) => sum + this.isolated([ ...branch, ...moved ]), 0);
    if (moved.length === 0 || after >= before) {
      return join;
    }

    const newUnion = this.algebraFactory.createUnion(union.input.map(branch => this.withPatterns(branch, moved)), false);
    const newOthers = others
      .map(input => this.withoutPatterns(input, moved))
      .filter((input): input is Algebra.Operation => input !== undefined);
    return newOthers.length === 0 ? newUnion : this.algebraFactory.createJoin([ ...newOthers, newUnion ], false);
  }

  /**
   * The outer patterns to move: those isolated beside the union that a branch gives a partner, and
   * every outer pattern about a subject that would give an isolated pattern of a branch a partner.
   */
  protected patternsToMove(outer: Algebra.Pattern[], branches: Algebra.Pattern[][]): Algebra.Pattern[] {
    const moved = new Set<Algebra.Pattern>();
    for (const pattern of outer) {
      if (this.isCandidate(pattern) && !this.hasPartner(pattern, outer) &&
        branches.some(branch => this.hasPartner(pattern, branch))) {
        moved.add(pattern);
      }
    }
    for (const branch of branches) {
      for (const pattern of branch) {
        if (!this.isCandidate(pattern) || this.hasPartner(pattern, branch)) {
          continue;
        }
        for (const partner of outer) {
          if (this.partners(pattern, partner)) {
            // Its whole subject group goes along, so the stars outside are not split up
            for (const member of outer) {
              if (member.subject.equals(partner.subject)) {
                moved.add(member);
              }
            }
          }
        }
      }
    }
    return [ ...moved ];
  }

  /**
   * The number of patterns with a variable subject that no other pattern of the group forms a star or
   * a chain of two with.
   */
  public isolated(patterns: Algebra.Pattern[]): number {
    return patterns.filter(pattern => this.isCandidate(pattern) && !this.hasPartner(pattern, patterns)).length;
  }

  protected isCandidate(pattern: Algebra.Pattern): boolean {
    return pattern.subject.termType === 'Variable';
  }

  protected hasPartner(pattern: Algebra.Pattern, group: Algebra.Pattern[]): boolean {
    return group.some(other => other !== pattern && this.partners(pattern, other));
  }

  /**
   * Whether two patterns form a star, sharing their subject, or a chain, one ending where the other
   * starts.
   */
  protected partners(left: Algebra.Pattern, right: Algebra.Pattern): boolean {
    return left.subject.equals(right.subject) ||
      left.subject.equals(right.object) ||
      left.object.equals(right.subject);
  }

  /**
   * The patterns of an input that is a BGP or a single pattern, or undefined for anything else.
   */
  protected patternsOf(operation: Algebra.Operation): Algebra.Pattern[] | undefined {
    if (operation.type === Algebra.Types.BGP) {
      return (<Algebra.Bgp> operation).patterns;
    }
    if (operation.type === Algebra.Types.PATTERN) {
      return [ <Algebra.Pattern> operation ];
    }
    return undefined;
  }

  protected withPatterns(branch: Algebra.Operation, moved: Algebra.Pattern[]): Algebra.Operation {
    // Every branch gets patterns of its own, as what is delegated is decided per operation: one shared
    // between branches would be handed to a composite of one branch and then count as taken in all
    const patterns = moved.map(pattern =>
      this.algebraFactory.createPattern(pattern.subject, pattern.predicate, pattern.object, pattern.graph));
    const own = this.patternsOf(branch);
    if (own) {
      return this.algebraFactory.createBgp([ ...own, ...patterns ]);
    }
    return this.algebraFactory.createJoin([ branch, this.algebraFactory.createBgp(patterns) ], false);
  }

  protected withoutPatterns(input: Algebra.Operation, patterns: Algebra.Pattern[]): Algebra.Operation | undefined {
    const own = this.patternsOf(input);
    if (!own) {
      return input;
    }
    const kept = own.filter(pattern => !patterns.includes(pattern));
    if (kept.length === own.length) {
      return input;
    }
    return kept.length === 0 ? undefined : this.algebraFactory.createBgp(kept);
  }
}
