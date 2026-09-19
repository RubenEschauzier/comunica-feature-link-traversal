import { ActorDerivedResourceExecute, IActionDerivedResourceExecute, IActorDerivedResourceExecuteOutput, IActorDerivedResourceExecuteArgs } from '@comunica/bus-derived-resource-execute';
import { ICompositeExecutionBlock } from '@comunica/bus-derived-resource-partition';
import { KeysRdfJoin } from '@comunica/context-entries';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, failTest, passTestVoid } from '@comunica/core';
import { Bindings } from '@comunica/types';
import { Algebra, AlgebraFactory } from '@comunica/utils-algebra';
import { isDomainDelimited, isWithinDomain } from '@comunica/actor-rdf-join-inner-multi-stems';
import type * as RDF from '@rdfjs/types';

/**
 * A comunica Composite Source Derived Resource Execute Actor. This hands every block of join entries
 * to its resource as one sub-query, and gives the resulting bindings to the adaptive join as a
 * composite source.
 */
export class ActorDerivedResourceExecuteCompositeSource extends ActorDerivedResourceExecute {
  protected readonly algebraFactory = new AlgebraFactory();

  public constructor(args: IActorDerivedResourceExecuteArgs) {
    super(args);
  }

  public async test(action: IActionDerivedResourceExecute): Promise<TestResult<IActorTest>> {
    if (this.compositeBlocks(action).length === 0) {
      return failTest(`${this.name}: no composite blocks to execute`);
    }
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourceExecute): Promise<IActorDerivedResourceExecuteOutput> {
    // TODO Use signal to abort dereference operation (is this needed for experiments?)
    const controller = new AbortController();

    const context = action.context;
    const manager = context.getSafe(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager);
    manager.addDereferencingDerivedResource(controller);

    const adaptiveJoinController = context.getSafe(KeysRdfJoin.adaptiveJoinController);

    // The blocks are disjoint over their operations, so they can all be sent without two composite
    // sources ever covering the same join entry
    await Promise.allSettled(this.compositeBlocks(action).map(async(block) => {
      // TODO: Future work: the resource should have a way of indicating its domain, this only
      // works if the domain of the resource is at where it resides.
      const domain = block.resource.baseUrl;
      const domainIsDelimited = isDomainDelimited(domain);

      // An anchor that is already bound to something outside the resource's authority puts the
      // whole block out of reach, so it is dropped rather than filtered
      const outOfDomain = block.anchorTerms.some(term => term.termType !== 'Variable' &&
        !isWithinDomain(term.value, domain, domainIsDelimited));

      if (outOfDomain) {
        return;
      }

      let bindingsStream = block.resource.querySource.queryBindings(
        this.asSingleOperation(block.operations),
        context,
      );

      // The anchors that are still variables are only known once bound, so those are checked per
      // binding. In chain ?s <p1> ?o1 . ?o1 <p2> ?o2 we need authority over ?s and ?o1, not ?o2
      const anchorVariables = <RDF.Variable[]> block.anchorTerms
        .filter(term => term.termType === 'Variable');
      if (anchorVariables.length > 0) {
        bindingsStream = bindingsStream.filter((binding: Bindings) => anchorVariables.every((variable) => {
          const term = binding.get(variable);
          return term !== undefined && isWithinDomain(term.value, domain, domainIsDelimited);
        }));
      }

      console.log("Adding block")
      console.log(block)
      adaptiveJoinController.addCompositeSource(block.operations, bindingsStream, {
        authoritativeDomain: domain,
        anchorTerms: block.anchorTerms,
      });

      // TODO: Think about how reachability works when we aggregate over data.
      // When we aggregate over something that is not reachable, we will still include
      // it in results so reachability becomes muddy. Some formalizations maybe,
      // maybe call it the hybrid cMatch - all criterion?
    }));

    manager.removeDereferencingDerivedResource(controller);
    return { links: []};
  }

  /**
   * The operations of a block as the single sub-query to ask the resource.
   */
  protected asSingleOperation(operations: Algebra.Operation[]): Algebra.Operation {
    const patterns = operations.filter((operation): operation is Algebra.Pattern =>
      operation.type === Algebra.Types.PATTERN);

    // A join of patterns is a bgp, which is what a resource selector shape is matched against
    return patterns.length === operations.length ?
      this.algebraFactory.createBgp(patterns) :
      this.algebraFactory.createJoin(operations);
  }
}
