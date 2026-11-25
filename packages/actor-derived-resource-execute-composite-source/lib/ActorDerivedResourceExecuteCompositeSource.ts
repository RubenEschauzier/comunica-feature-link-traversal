import { ActorDerivedResourceExecute, IActionDerivedResourceExecute, IActorDerivedResourceExecuteOutput, IActorDerivedResourceExecuteArgs } from '@comunica/bus-derived-resource-execute';
import { ICompositeExecutionBlock } from '@comunica/bus-derived-resource-partition';
import { skolemizeTerm } from '@comunica/actor-optimize-query-operation-query-source-skolemize';
import { KeysInitQuery, KeysQueryOperation, KeysQuerySourceIdentify, KeysRdfJoin } from '@comunica/context-entries';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, failTest, passTestVoid } from '@comunica/core';
import type {
  Bindings,
  BindingsStream,
  ComunicaDataFactory,
  FragmentSelectorShape,
  IActionContext,
  MetadataBindings,
} from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { ArrayIterator } from 'asynciterator';
import type { AsyncIterator } from 'asynciterator';
import { doesShapeAcceptOperation } from '@comunica/utils-query-operation';
import type { IBaseTupleSource } from './BoundCompositeCoordinator';
import { BoundCompositeCoordinator } from './BoundCompositeCoordinator';
import { ExtendedBlockSplit } from './ExtendedBlockSplit';
import { FilterPushdown } from './FilterPushdown';
import type { IAggregatedStore, ILinkTraversalManager } from '@comunica/types-link-traversal';
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
  protected readonly bindingsFactory = new BindingsFactory(this.dataFactory);

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
    const manager = action.context.getSafe(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager);
    manager.addDereferencingDerivedResource(controller);
    try {
      // console.log(`Running: ${JSON.stringify(action.executionBlocks.map(x => (<ICompositeExecutionBlock>x).resource))}`)
      return await this.runBlocks(action, manager);
    } finally {
      // Traversal only ends once no derived resource is dereferencing, so a failing one must not
      // stay registered or the query never ends
      manager.removeDereferencingDerivedResource(controller);
      // Every composite request has been sent, or none will be, so waiting on them can begin
      action.compositeResponses?.close();
    }
  }

  protected async runBlocks(
    action: IActionDerivedResourceExecute,
    manager: ILinkTraversalManager,
  ): Promise<IActorDerivedResourceExecuteOutput> {
    const context = action.context;
    const adaptiveJoinController = context.getSafe(KeysRdfJoin.adaptiveJoinController);
    const store = manager.getAggregatedStore();
    const skolemId = this.storeSkolemId(context);

    const bound = ActorDerivedResourceExecuteCompositeSource.boundComposites();
    const coordinator = bound ? BoundCompositeCoordinator.forQuery(adaptiveJoinController, manager) : undefined;

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

      // Bound composites: a block without a constant sharing a variable with the region of one is asked
      // only for the values the blocks with a constant (seeds) bind, when those settle at once (one-shot)
      let seedVariables: RDF.Variable[] = [];
      if (coordinator) {
        const role = await this.boundRole(block, adaptiveJoinController, context);
        if (process.env.DERIVED_BOUND_DEBUG === '1') {
          process.stderr.write(`[bound] ${domain} ${block.operations.length} ops: ${role?.target ? `target ?${role.target.value}` : role?.seed ? `seed ${role.seed.map(v => `?${v.value}`).join(' ')}` : 'none'}\n`);
        }
        // Seed values come from the base operators of the patterns around the constants as well, which hold
        // the values of every source, not only of those answering composite blocks
        this.tapRegion(block, adaptiveJoinController, coordinator);
        if (role?.target) {
          const target = role.target;
          const send = (values: RDF.Term[]): Promise<number> =>
            this.sendBound(block, target, values, adaptiveJoinController, context, store, skolemId);
          const sendWhole = (): Promise<number> =>
            this.sendWhole(block, adaptiveJoinController, context, store, skolemId);
          const taken = coordinator.addTarget(target, send, { label: domain, sendWhole });
          if (process.env.DERIVED_BOUND_DEBUG === '1') {
            process.stderr.write(`[bound] ${domain} target ${taken ? 'bound' : 'whole'} ${JSON.stringify(coordinator.statistics)}\n`);
          }
          if (taken) {
            // Its requests go as seed values come; the block is not accepted, as it is answered only for those
            return;
          }
        } else if (role?.seed) {
          seedVariables = role.seed;
        }
      }

      if (block.core) {
        await this.sendExtended(block, block.core, action, adaptiveJoinController, store, skolemId, {
          coordinator,
          seedVariables,
        });
        return;
      }

      const answers = block.resource.querySource.queryBindings(
        this.blockQuery(block, context),
        context,
      );
      const responded = answers.getProperty<Promise<void>>('responded');
      if (responded) {
        action.compositeResponses?.add(responded);
      }
      let bindingsStream = this.traceWhole(
        block,
        this.withinDomain(block, this.scopeBlankNodes(answers, domain, store, skolemId)),
      );
      let releaseSeed: (() => void) | undefined;
      if (coordinator && seedVariables.length > 0) {
        const tapped = coordinator.tapSeed(bindingsStream, seedVariables, domain);
        bindingsStream = tapped.stream;
        releaseSeed = tapped.release;
      }

      // Only add the non-composite filters to qurey execution once composite has responded.
      // If composite is delayed we still execute query normally allowing us to produce results locally.
      if (responded) {
        await responded;
      }

      const accepted = adaptiveJoinController.addCompositeSource(block.operations, bindingsStream, {
        authoritativeDomain: domain,
        anchorTerms: block.anchorTerms,
      });

      if (accepted) {
        action.compositeResponses?.accept(block);
      } else {
        // The join never reads it, so it ends nothing
        releaseSeed?.();
      }

      // TODO: Think about how reachability works when we aggregate over data.
      // When we aggregate over something that is not reachable, we will still include
      // it in results so reachability becomes muddy. Some formalizations maybe,
      // maybe call it the hybrid cMatch - all criterion?
    }));

    return { links: []};
  }

  /**
   * Requests an extended block whole, as the extended block and the star it was extended from, which the join
   * takes on as two composite sources over the pod (see ExtendedBlockSplit). A resource taking an OPTIONAL is
   * asked once, for `star OPTIONAL { hops }`; any other resource is asked for the star and the extended block
   * separately.
   */
  protected async sendExtended(
    block: ICompositeExecutionBlock,
    core: { operations: Algebra.Operation[]; anchorTerms: RDF.Term[] },
    action: IActionDerivedResourceExecute,
    adaptiveJoinController: {
      addCompositeSource: (operations: Algebra.Operation[], stream: BindingsStream, metadata?: Record<string, any>) => boolean;
    },
    store: IAggregatedStore,
    skolemId: string | undefined,
    seeds: { coordinator?: BoundCompositeCoordinator; seedVariables: RDF.Variable[] },
  ): Promise<void> {
    const context = action.context;
    const domain = block.resource.baseUrl;
    const starBlock: ICompositeExecutionBlock = {
      type: 'composite',
      operations: core.operations,
      resource: block.resource,
      anchorTerms: core.anchorTerms,
    };
    const termKey = (term: RDF.Term): string => `${term.termType}|${term.value}`;
    const coreAnchors = new Set(core.anchorTerms.map(termKey));
    const hubs = <RDF.Variable[]> block.anchorTerms
      .filter(term => term.termType === 'Variable' && !coreAnchors.has(termKey(term)));
    const corePatterns = <Algebra.Pattern[]> core.operations.filter(op => op.type === Algebra.Types.PATTERN);
    const hops = <Algebra.Pattern[]> block.operations
      .filter(op => op.type === Algebra.Types.PATTERN && !core.operations.includes(op));
    const variablesOf = (patterns: Algebra.Pattern[]): RDF.Variable[] => [ ...new Map(patterns
      .flatMap(pattern => [ pattern.subject, pattern.predicate, pattern.object ])
      .filter((term): term is RDF.Variable => term.termType === 'Variable')
      .map(term => [ term.value, term ])).values() ];

    const responses: Promise<void>[] = [];
    const releases: (() => void)[] = [];
    const request = (requested: ICompositeExecutionBlock, operation?: Algebra.Operation): BindingsStream => {
      const answers = block.resource.querySource.queryBindings(
        this.blockQuery(requested, context, operation),
        context,
      );
      const responded = answers.getProperty<Promise<void>>('responded');
      if (responded) {
        action.compositeResponses?.add(responded);
        responses.push(responded);
      }
      let rows = this.withinDomain(requested, this.scopeBlankNodes(answers, domain, store, skolemId));
      const variables = new Set(variablesOf(<Algebra.Pattern[]> requested.operations).map(variable => variable.value));
      const seedVariables = seeds.seedVariables.filter(variable => variables.has(variable.value));
      if (seeds.coordinator && seedVariables.length > 0) {
        const tapped = seeds.coordinator.tapSeed(rows, seedVariables, domain);
        rows = tapped.stream;
        releases.push(tapped.release);
      }
      return rows;
    };

    // The marker is bound within the OPTIONAL, so a row tells whether its hops matched also when they bind no
    // variable of their own
    const marker = this.dataFactory.variable('derivedExtendedBlockHop');
    const optional = this.algebraFactory.createLeftJoin(
      this.algebraFactory.createBgp(corePatterns),
      this.algebraFactory.createExtend(
        this.algebraFactory.createBgp(hops),
        marker,
        this.algebraFactory.createTermExpression(this.dataFactory.literal(
          'true',
          this.dataFactory.namedNode('http://www.w3.org/2001/XMLSchema#boolean'),
        )),
      ),
    );
    const coreVariables = variablesOf(corePatterns).map(variable => variable.value);
    let split: { star: AsyncIterator<Bindings>; extended: AsyncIterator<Bindings> };
    if (doesShapeAcceptOperation(await block.resource.querySource.getSelectorShape(context), optional)) {
      split = new ExtendedBlockSplit(coreVariables, hubs, domain, marker)
        .splitOptional(request(starBlock, optional));
    } else {
      split = new ExtendedBlockSplit(coreVariables, hubs, domain)
        .splitPair(request(starBlock), request(block));
    }

    // Only add the non-composite filters to query execution once composite has responded
    await Promise.all(responses);

    // Both are blocks of the same join component, so the join takes on both or neither, as it has to: neither
    // holds all star rows
    const extendedTaken = adaptiveJoinController.addCompositeSource(
      block.operations,
      <BindingsStream> this.traceWhole(block, <BindingsStream> split.extended),
      { authoritativeDomain: domain, anchorTerms: block.anchorTerms },
    );
    const starTaken = adaptiveJoinController.addCompositeSource(
      core.operations,
      <BindingsStream> this.traceWhole(starBlock, <BindingsStream> split.star),
      { authoritativeDomain: domain, anchorTerms: core.anchorTerms },
    );
    if (extendedTaken) {
      action.compositeResponses?.accept(block);
    } else {
      split.extended.destroy();
    }
    if (starTaken) {
      action.compositeResponses?.accept(starBlock);
    } else {
      split.star.destroy();
    }
    if (!extendedTaken && !starTaken) {
      // The join never reads them, so they end nothing
      for (const release of releases) {
        release();
      }
    }
  }

  /**
   * Whether bound composites are on: DERIVED_BOUND_COMPOSITES=1.
   */
  public static boundComposites(): boolean {
    return process.env.DERIVED_BOUND_COMPOSITES === '1';
  }

  /**
   * The answers of a composite request whose anchors lie within the resource's domain: the resource is
   * authoritative for nothing else, so the base operators answer the rest.
   */
  protected withinDomain(block: ICompositeExecutionBlock, bindingsStream: BindingsStream): BindingsStream {
    const domain = block.resource.baseUrl;
    const domainIsDelimited = isDomainDelimited(domain);
    // Variables required to be within authority of composite source
    const anchorVariables = <RDF.Variable[]> block.anchorTerms
      .filter(term => term.termType === 'Variable');
    if (anchorVariables.length === 0) {
      return bindingsStream;
    }
    return bindingsStream.filter((binding: Bindings) => anchorVariables.every((variable) => {
      const term = binding.get(variable);
      // If binding for variable exist and is within domain or blank node we keep it.
      // Blank nodes are scoped to the source and assumed to be equal between the composite source
      // and non-composite sources aggregated by composite
      return term !== undefined &&
        (term.termType === 'BlankNode' || isWithinDomain(term.value, domain, domainIsDelimited));
    }));
  }

  /**
   * Taps the base operators of the block's join component for seed values, following the component's seed plan
   * (once per component).
   */
  protected tapRegion(
    block: ICompositeExecutionBlock,
    adaptiveJoinController: { getComponentsForOperations: (operations: Algebra.Operation[]) => unknown[] },
    coordinator: BoundCompositeCoordinator,
  ): void {
    const component = <{ operations: Algebra.Operation[]; stemsControllerStream?: Partial<IBaseTupleSource> } | undefined>
      adaptiveJoinController.getComponentsForOperations(block.operations)[0];
    const controller = component?.stemsControllerStream;
    if (controller?.onBaseTuples) {
      coordinator.tapRegion(<IBaseTupleSource> controller, component!.operations);
    }
  }

  /**
   * The rows of a block requested whole, counted in a trace once read in full (DERIVED_BOUND_TRACE=1).
   */
  protected traceWhole(block: ICompositeExecutionBlock, stream: BindingsStream): BindingsStream {
    if (!BoundCompositeCoordinator.TRACE) {
      return stream;
    }
    const sentAt = Date.now();
    let rows = 0;
    const counted = stream.map((binding) => {
      rows++;
      return binding;
    });
    counted.on('end', () => BoundCompositeCoordinator.trace({
      e: 'whole',
      pod: block.resource.baseUrl,
      ops: block.operations.length,
      rows,
      ms: Date.now() - sentAt,
    }));
    return counted;
  }

  /**
   * What a block is to bound composites: a seed, binding the given variables of its region, when it holds a
   * constant; a target, bound on the given variable, when it shares one with the region of a constant and its
   * resource takes bindings to join with; otherwise nothing.
   */
  protected async boundRole(
    block: ICompositeExecutionBlock,
    adaptiveJoinController: { getComponentsForOperations: (operations: Algebra.Operation[]) => { operations: Algebra.Operation[] }[] },
    context: IActionContext,
  ): Promise<{ seed?: RDF.Variable[]; target?: RDF.Variable } | undefined> {
    const component = adaptiveJoinController.getComponentsForOperations(block.operations)[0];
    if (component === undefined) {
      return undefined;
    }
    const seedable = BoundCompositeCoordinator.seedableVariables(component.operations);
    const patterns = <Algebra.Pattern[]> block.operations.filter(operation => operation.type === Algebra.Types.PATTERN);
    const variables = new Map<string, RDF.Variable>();
    for (const pattern of patterns) {
      for (const term of [ pattern.subject, pattern.object ]) {
        if (term.termType === 'Variable') {
          variables.set(term.value, term);
        }
      }
    }
    const shared = [ ...variables.values() ].filter(variable => seedable.has(variable.value));
    if (patterns.some(pattern => pattern.subject.termType !== 'Variable' || pattern.object.termType !== 'Variable')) {
      return shared.length > 0 ? { seed: shared } : undefined;
    }
    if (shared.length === 0 || !this.takesJoinBindings(await block.resource.querySource.getSelectorShape(context))) {
      return undefined;
    }
    // Binding a variable that is no anchor is what traversal cannot do: look up a block by an object
    const anchors = new Set(block.anchorTerms.map(term => term.value));
    return { target: shared.find(variable => !anchors.has(variable.value)) ?? shared[0] };
  }

  /**
   * Whether the selector shape takes bindings to join with anywhere, as the SPARQL source's does on its
   * operations, nested within a conjunction and disjunction.
   */
  protected takesJoinBindings(shape: FragmentSelectorShape): boolean {
    if ((<{ joinBindings?: boolean }> shape).joinBindings === true) {
      return true;
    }
    const children = (<{ children?: FragmentSelectorShape[] }> shape).children;
    return children !== undefined && children.some(child => this.takesJoinBindings(child));
  }

  /**
   * Sends one bound request for the block, joined with the given values of the variable, and hands its answer
   * to the join with a claim for those values. Resolves once the answer has been read in full.
   */
  protected sendBound(
    block: ICompositeExecutionBlock,
    variable: RDF.Variable,
    values: RDF.Term[],
    adaptiveJoinController: { addCompositeSource: (operations: Algebra.Operation[], stream: BindingsStream, metadata?: Record<string, any>) => boolean },
    context: IActionContext,
    store: IAggregatedStore,
    skolemId: string | undefined,
  ): Promise<number> {
    const bindings = values.map(value => this.bindingsFactory.bindings([[ variable, value ]]));
    const answers = block.resource.querySource.queryBindings(
      this.blockQuery(block, context),
      context,
      { joinBindings: {
        bindings: <BindingsStream> <unknown> new ArrayIterator(bindings, { autoStart: false }),
        metadata: <MetadataBindings> <unknown> {
          state: undefined,
          cardinality: { type: 'exact', value: bindings.length },
          variables: [{ variable, canBeUndef: false }],
        },
      }},
    );
    let rows = 0;
    const stream = this.withinDomain(block, this.scopeBlankNodes(answers, block.resource.baseUrl, store, skolemId))
      .map((binding) => {
        rows++;
        return binding;
      });
    const ended = new Promise<number>((resolve, reject) => {
      stream.on('end', () => resolve(rows));
      stream.on('error', reject);
    });
    const accepted = adaptiveJoinController.addCompositeSource(block.operations, stream, {
      authoritativeDomain: block.resource.baseUrl,
      anchorTerms: block.anchorTerms,
      boundVariable: variable,
      boundValues: values,
    });
    if (!accepted) {
      stream.destroy();
      return Promise.resolve(0);
    }
    return ended;
  }

  /**
   * Requests a target's block whole, after its bound requests failed, and hands the answer to the join like any
   * composite answer. It is not accepted for the store-level drop, as that is decided when the pod's resources
   * are executed. Resolves with the number of rows once the answer has been read in full.
   */
  protected sendWhole(
    block: ICompositeExecutionBlock,
    adaptiveJoinController: {
      addCompositeSource: (operations: Algebra.Operation[], stream: BindingsStream, metadata?: Record<string, any>) => boolean;
    },
    context: IActionContext,
    store: IAggregatedStore,
    skolemId: string | undefined,
  ): Promise<number> {
    const answers = block.resource.querySource.queryBindings(this.blockQuery(block, context), context);
    let rows = 0;
    const stream = this.withinDomain(block, this.scopeBlankNodes(answers, block.resource.baseUrl, store, skolemId))
      .map((binding) => {
        rows++;
        return binding;
      });
    const ended = new Promise<number>((resolve, reject) => {
      stream.on('end', () => resolve(rows));
      stream.on('error', reject);
    });
    const accepted = adaptiveJoinController.addCompositeSource(block.operations, stream, {
      authoritativeDomain: block.resource.baseUrl,
      anchorTerms: block.anchorTerms,
    });
    if (!accepted) {
      stream.destroy();
      return Promise.resolve(0);
    }
    return ended;
  }

  /**
   * Ensure blank nodes of composite sources and non-composite sources aggregated by the composite
   * are skolemized the same way.
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
   * The query to ask the resource for a block: its operations (or the given operation over them), with the
   * query's filters that apply to every solution of the block and mention only its variables (see
   * FilterPushdown).
   */
  protected blockQuery(
    block: ICompositeExecutionBlock,
    context: IActionContext,
    operation: Algebra.Operation = this.asSingleOperation(block.operations),
  ): Algebra.Operation {
    const query = context.get(KeysInitQuery.query);
    const patterns = block.operations.filter((op): op is Algebra.Pattern => op.type === Algebra.Types.PATTERN);
    if (!query || typeof query !== 'object' || !('type' in query) || patterns.length !== block.operations.length) {
      return operation;
    }
    const filters = FilterPushdown.pushableFilters(<Algebra.Operation> query, patterns);
    if (process.env.DERIVED_FILTER_DEBUG === '1') {
      process.stderr.write(`[filter] ${block.resource.baseUrl} ${patterns.length} patterns: ${filters.length} filters\n`);
    }
    return filters.reduce<Algebra.Operation>(
      (input, expression) => this.algebraFactory.createFilter(input, expression),
      operation,
    );
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
