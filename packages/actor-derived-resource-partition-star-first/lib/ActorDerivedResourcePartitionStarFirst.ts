import { ActorDerivedResourcePartition, IActionDerivedResourcePartition, IActorDerivedResourcePartitionOutput, IActorDerivedResourcePartitionArgs, IActorDerivedResourcePartitionTest, ICompositeExecutionBlock, ITriplePatternExecutionBlock } from '@comunica/bus-derived-resource-partition';
import { ICandidateResource } from '@comunica/bus-derived-resource-propose';
import { TestResult, passTest } from '@comunica/core';
import { canShapeAnswerBgp } from '@comunica/bus-derived-resource-propose';
import type { FragmentSelectorShape } from '@comunica/types';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';

/**
 * A comunica Star First Derived Resource Partition Actor.
 */
export class ActorDerivedResourcePartitionStarFirst extends ActorDerivedResourcePartition {
  protected readonly algebraFactory = new AlgebraFactory();

  public constructor(args: IActorDerivedResourcePartitionArgs) {
    super(args);
  }

  public async test(action: IActionDerivedResourcePartition): Promise<TestResult<IActorDerivedResourcePartitionTest>> {
    return passTest({ accuracy: .5 });
  }

  public async run(action: IActionDerivedResourcePartition): Promise<IActorDerivedResourcePartitionOutput> {
    const proposedResources = await this.mediatorDerivedResourcePropose.mediate(action);

    const composite: ICandidateResource[] = [];
    const triplePattern: ICandidateResource[] = [];
    for (const candidate of proposedResources.candidateResources) {
      (candidate.kind === 'triple-pattern' ? triplePattern : composite).push(candidate);
    }

    return {
      resourceExecutionBlocks: [
        ...await this.partitionComposite(action, composite),
        ...this.groupTriplePatterns(triplePattern),
      ],
    };
  }

  /**
   * The composite blocks, which have to come out disjoint over their operations.
   */
  protected async partitionComposite(
    action: IActionDerivedResourcePartition,
    candidates: ICandidateResource[],
  ): Promise<ICompositeExecutionBlock[]> {
    const ranked = [ ...candidates ].sort((left, right) =>
      this.tier(left) - this.tier(right) || this.compare(left, right));

    // The proposers can produce overlapping candidates, so the partition is built by walking
    // them best-first and handing every operation to the first candidate that claims it
    const claimed = new Set<Algebra.Operation>();
    const blocks: ICompositeExecutionBlock[] = [];
    for (const candidate of ranked) {
      const unclaimed = candidate.operations.filter(operation => !claimed.has(operation));

      // A candidate was matched against its resource as a whole. If parts are claimed
      // the propose actor must once again try to match a resource against the new operation
      let block = candidate;
      if (unclaimed.length < candidate.operations.length) {
        const reduced = await this.retryReducedOperation(action, unclaimed);
        if (!reduced) {
          continue;
        }
        block = reduced;
      }

      for (const operation of block.operations) {
        claimed.add(operation);
      }
      blocks.push({
        type: 'composite',
        operations: block.operations,
        resource: block.resource,
        anchorTerms: block.anchorTerms,
      });
    }
    if (process.env.DERIVED_EXTEND_BLOCKS === '1') {
      return await this.extendBlocks(action, blocks, claimed);
    }
    return blocks;
  }

  /**
   * Extends every composite block with the stars hanging off its object variables (a variable in an object
   * position of the block that is the subject of no pattern in it), where none of the star's patterns is
   * claimed yet and the resource still answers the larger block. A star with an object variable leading to a
   * pattern of its pod, such as an offer whose listing names the product, then forms one block with it, whose
   * only variable shared with the rest of the query is that pattern's object. Such a block can be bound on
   * that variable (bound composites), and its rows need no join with the hanging pattern on the client.
   * A pattern held by a block of its own is taken over, and that block dropped: on its own it is the request
   * a pattern-level resource makes as well, while within the larger block it no longer needs one.
   */
  protected async extendBlocks(
    action: IActionDerivedResourcePartition,
    blocks: ICompositeExecutionBlock[],
    claimed: Set<Algebra.Operation>,
  ): Promise<ICompositeExecutionBlock[]> {
    const single = new Map<Algebra.Operation, ICompositeExecutionBlock>();
    for (const block of blocks) {
      if (block.operations.length === 1) {
        single.set(block.operations[0], block);
      }
    }
    const absorbed = new Set<ICompositeExecutionBlock>();
    const queryPatterns: Algebra.Pattern[] = [];
    algebraUtils.visitOperation(action.operation, {
      [Algebra.Types.PATTERN]: { visitor: (pattern) => {
        queryPatterns.push(pattern);
      } },
    });
    const key = (term: { termType: string; value: string }): string => `${term.termType}|${term.value}`;
    for (const block of blocks) {
      const patterns = <Algebra.Pattern[]> block.operations.filter(operation => operation.type === Algebra.Types.PATTERN);
      if (patterns.length !== block.operations.length || patterns.length < 2 || absorbed.has(block)) {
        continue;
      }
      const subjects = new Set(patterns.map(pattern => key(pattern.subject)));
      const hubs = new Map(patterns
        .filter(pattern => pattern.object.termType === 'Variable' && !subjects.has(key(pattern.object)))
        .map(pattern => [ key(pattern.object), pattern.object ]));
      let shape: FragmentSelectorShape | undefined;
      for (const [ hubKey, hub ] of hubs) {
        const star = queryPatterns.filter(pattern => key(pattern.subject) === hubKey);
        const takeable = (pattern: Algebra.Pattern): boolean => !claimed.has(pattern) ||
          (single.has(pattern) && !absorbed.has(single.get(pattern)!));
        if (star.length === 0 || star.some(pattern => !takeable(pattern) || pattern.predicate.termType !== 'NamedNode')) {
          continue;
        }
        shape ??= await block.resource.querySource.getSelectorShape(action.context);
        if (!canShapeAnswerBgp(shape, this.algebraFactory.createBgp([ ...<Algebra.Pattern[]> block.operations, ...star ]))) {
          continue;
        }
        for (const pattern of star) {
          claimed.add(pattern);
          const own = single.get(pattern);
          if (own) {
            absorbed.add(own);
          }
        }
        block.operations = [ ...block.operations, ...star ];
        block.anchorTerms = [ ...block.anchorTerms, hub ];
      }
    }
    return blocks.filter(block => !absorbed.has(block));
  }

  /**
   * The traversal patterns, with every resource that offered to serve each of them. These are left
   * overlapping: the aggregated store de-duplicates, so which resource to spend a request on stays
   * a decision for the actor that runs the block.
   */
  protected groupTriplePatterns(candidates: ICandidateResource[]): ITriplePatternExecutionBlock[] {
    const blocks = new Map<string, ITriplePatternExecutionBlock>();
    for (const candidate of candidates) {
      const pattern = <Algebra.Pattern> candidate.operations[0];
      const key = [ pattern.subject, pattern.predicate, pattern.object, pattern.graph ]
        .map(term => `${term.termType}:${term.value}`).join('|');

      const block = blocks.get(key);
      if (block) {
        block.resources.push(candidate.resource);
      } else {
        blocks.set(key, { type: 'triple-pattern', pattern, resources: [ candidate.resource ]});
      }
    }
    return [ ...blocks.values() ];
  }

  /**
   * Try to find matching resource for reduced operation
   */
  protected async retryReducedOperation(
    action: IActionDerivedResourcePartition,
    operations: Algebra.Operation[],
  ): Promise<ICandidateResource | undefined> {
    if (operations.length < 2) {
      return undefined;
    }

    const { candidateResources } = await this.mediatorDerivedResourcePropose.mediate({
      ...action,
      operation: this.asSingleOperation(operations),
    });

    return [ ...candidateResources ]
      .filter(candidate => candidate.operations.length > 1)
      .sort((left, right) => this.tier(left) - this.tier(right) || this.compare(left, right))[0];
  }

  protected asSingleOperation(operations: Algebra.Operation[]): Algebra.Operation {
    const patterns = operations.filter((operation): operation is Algebra.Pattern =>
      operation.type === Algebra.Types.PATTERN);

    // A join of patterns is a bgp, and the proposers only take the bgp apart again
    return patterns.length === operations.length ?
      this.algebraFactory.createBgp(patterns) :
      this.algebraFactory.createJoin(operations);
  }

  protected tier(candidate: ICandidateResource): number {
    if (candidate.kind !== 'star') {
      return 1;
    }
    return candidate.operations.length > 1 ? 0 : 2;
  }

  /**
   * Orders candidates of the same tier, best first.
   */
  protected compare(left: ICandidateResource, right: ICandidateResource): number {
    return right.features.patternCount - left.features.patternCount ||
      // A bound term makes the sub-query more selective, so the resource answers with less for the
      // rest of the query to join over, and the block does not wait on another block to bind it
      right.features.constantCount - left.features.constantCount;
  }
}
