import { ActorDerivedResourceExecute, IActionDerivedResourceExecute, IActorDerivedResourceExecuteOutput, IActorDerivedResourceExecuteArgs } from '@comunica/bus-derived-resource-execute';
import { ICompositeExecutionBlock } from '@comunica/bus-derived-resource-partition';
import { skolemizeTerm } from '@comunica/actor-optimize-query-operation-query-source-skolemize';
import { KeysQueryOperation, KeysQuerySourceIdentify, KeysRdfJoin } from '@comunica/context-entries';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, failTest, passTestVoid } from '@comunica/core';
import type { Bindings, BindingsStream, ComunicaDataFactory, IActionContext } from '@comunica/types';
import type { IAggregatedStore } from '@comunica/types-link-traversal';
import { Algebra, AlgebraFactory } from '@comunica/utils-algebra';
import { isDomainDelimited, isWithinDomain } from '@comunica/actor-rdf-join-inner-multi-stems';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';

/**
 * A comunica Composite Source Derived Resource Execute Actor. This hands every block of join entries
 * to its resource as one sub-query, and gives the resulting bindings to the adaptive join as a
 * composite source.
 */
export class ActorDerivedResourceExecuteCompositeSource extends ActorDerivedResourceExecute {
  protected readonly algebraFactory = new AlgebraFactory();
  protected readonly dataFactory: ComunicaDataFactory = new DataFactory();

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
    try {
      return await this.runBlocks(action);
    } finally {
      // Every composite request has been sent, or none will be, so waiting on them can begin
      action.compositeResponses?.close();
    }
  }

  protected async runBlocks(action: IActionDerivedResourceExecute): Promise<IActorDerivedResourceExecuteOutput> {
    // TODO Use signal to abort dereference operation (is this needed for experiments?)
    const controller = new AbortController();

    const context = action.context;
    const manager = context.getSafe(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager);
    manager.addDereferencingDerivedResource(controller);

    const adaptiveJoinController = context.getSafe(KeysRdfJoin.adaptiveJoinController);
    const store = manager.getAggregatedStore();
    const skolemId = this.storeSkolemId(context);

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

      const answers = block.resource.querySource.queryBindings(
        this.asSingleOperation(block.operations),
        context,
      );
      const responded = answers.getProperty<Promise<void>>('responded');
      if (responded) {
        action.compositeResponses?.add(responded);
      }
      let bindingsStream = this.scopeBlankNodes(answers, block.resource.baseUrl, store, skolemId);

      // The anchors that are still variables are only known once bound, so those are checked per
      // binding. In chain ?s <p1> ?o1 . ?o1 <p2> ?o2 we need authority over ?s and ?o1, not ?o2.
      // A blank node has no url to check, but one answered by this resource is a node of the container
      // it aggregates, as its server labels the blank nodes there the same in all of them
      const anchorVariables = <RDF.Variable[]> block.anchorTerms
        .filter(term => term.termType === 'Variable');
      if (anchorVariables.length > 0) {
        bindingsStream = bindingsStream.filter((binding: Bindings) => anchorVariables.every((variable) => {
          const term = binding.get(variable);
          return term !== undefined &&
            (term.termType === 'BlankNode' || isWithinDomain(term.value, domain, domainIsDelimited));
        }));
      }

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
   * Gives the blank nodes in the answers of a resource the terms the join sees for them elsewhere.
   * The resource hands out the labels its server gives the blank nodes of the documents it aggregates,
   * which the aggregated store maps to the term that blank node has in those documents and in the
   * other resources over them. The join reads the store
   * through a skolemized source, so the same skolemization is applied here.
   */
  protected scopeBlankNodes(
    answers: BindingsStream,
    source: string,
    store: IAggregatedStore,
    skolemId: string | undefined,
  ): BindingsStream {
    if (!store.scopeStoredBlankNode) {
      return answers;
    }
    const scopeTerm = (term: RDF.Term): RDF.Term => {
      if (term.termType !== 'BlankNode') {
        return term;
      }
      const scoped = store.scopeStoredBlankNode!(term.value, source);
      return skolemId === undefined ? scoped : skolemizeTerm(this.dataFactory, scoped, skolemId);
    };
    return answers.map(bindings => bindings.map(term => scopeTerm(term)));
  }

  /**
   * The id the skolemized source over the aggregated store relabels its blank nodes with. Traversal
   * queries all data through that one source, so it is the only source an id was handed out to. The
   * context of link extraction carries the ids, though not always the sources themselves.
   */
  protected storeSkolemId(context: IActionContext): string | undefined {
    const sourceIds = context.get(KeysQuerySourceIdentify.sourceIds);
    if (sourceIds?.size === 1) {
      return [ ...sourceIds.values() ][0];
    }
    const ids = (context.get(KeysQueryOperation.querySources) ?? [])
      .map(wrapper => (<{ sourceId?: string }> wrapper.source).sourceId)
      .filter((id): id is string => id !== undefined);
    return ids.length === 1 ? ids[0] : undefined;
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
