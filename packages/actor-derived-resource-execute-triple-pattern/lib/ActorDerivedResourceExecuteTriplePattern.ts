import type { IDerivedResource, IDerivedResourceCoefficients } from '@comunica/actor-extract-links-solid-derived-resources';
import type { IActionDerivedResourceExecute, IActorDerivedResourceExecuteOutput, IActorDerivedResourceExecuteArgs } from '@comunica/bus-derived-resource-execute';
import { ActorDerivedResourceExecute } from '@comunica/bus-derived-resource-execute';
import type { IBatchQuerySource } from '@comunica/bus-derived-resource-identify';
import type { ICompositeExecutionBlock } from '@comunica/bus-derived-resource-partition';
import { isBatchQuerySource } from '@comunica/bus-derived-resource-identify';
import type { IActorExtractLinksOutput, MediatorExtractLinks } from '@comunica/bus-extract-links';
import type { IActorRdfMetadataOutput, MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import type { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import { KeysInitQuery, KeysRdfJoin } from '@comunica/context-entries';
import { KeysQuerySourceIdentifyLinkTraversal, KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import type { TestResult, IActorTest } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
import type { ComunicaDataFactory, ILink } from '@comunica/types';
import type { ILinkTraversalManager } from '@comunica/types-link-traversal';
import { isDomainDelimited, isWithinDomain } from '@comunica/actor-rdf-join-inner-multi-stems';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import { wrap } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import type { QuadTermName } from 'rdf-terms';
import { filterQuadTermNames, matchPatternComplete, matchPatternMappings } from 'rdf-terms';

const DF = new DataFactory<RDF.BaseQuad>();
const VAR_SUBJ = DF.variable('__comunica:pp_var_subj');
const VAR_PRED = DF.variable('__comunica:pp_var_pred');
const VAR_OBJ = DF.variable('__comunica:pp_var_obj');

/**
 * A comunica Triple Pattern Derived Resource Execute Actor. This serves single patterns from a
 * derived resource instead of crawling for them, importing whatever also answers the query into the
 * aggregated store and returning the links found along the way.
 */
export class ActorDerivedResourceExecuteTriplePattern extends ActorDerivedResourceExecute {
  protected dataFactory: ComunicaDataFactory = new DataFactory();
  protected algebraFactory: AlgebraFactory = new AlgebraFactory(this.dataFactory);

  public readonly mediatorMetadata: MediatorRdfMetadata;
  public readonly mediatorExtractLinks: MediatorExtractLinks;
  public readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;
  protected derivedResourceCoefficients: IDerivedResourceCoefficients;

  public constructor(args: IActorDerivedResourceExecuteTriplePatternArgs) {
    super(args);
    this.mediatorMetadata = args.mediatorMetadata;
    this.mediatorExtractLinks = args.mediatorExtractLinks;
    this.mediatorMetadataExtract = args.mediatorMetadataExtract;
    this.derivedResourceCoefficients = args.derivedResourceCoefficients;
  }

  public async test(action: IActionDerivedResourceExecute): Promise<TestResult<IActorTest>> {
    if (this.triplePatternBlocks(action).length === 0) {
      return failTest(`${this.name}: no triple pattern blocks to execute`);
    }
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourceExecute): Promise<IActorDerivedResourceExecuteOutput> {
    // TODO: Use signal to abort!!
    const controller = new AbortController();
    const manager = action.context.getSafe(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager);
    manager.addDereferencingDerivedResource(controller);
    try {
      return await this.runBlocks(action, manager);
    } finally {
      // Traversal only ends once no derived resource is dereferencing, so a failing one must not
      // stay registered or the query never ends
      manager.removeDereferencingDerivedResource(controller);
    }
  }

  protected async runBlocks(
    action: IActionDerivedResourceExecute,
    manager: ILinkTraversalManager,
  ): Promise<IActorDerivedResourceExecuteOutput> {
    const context = action.context;
    const query = context.get(KeysInitQuery.query);
    const queryPatterns = query ? this.extractPatternsQuery(query) : [];
    const hasWildcardQueryPattern = queryPatterns.some(pattern => this.isWildcardPattern(pattern));

    // A block carries every resource that offered to serve its pattern, so the cheapest one is the
    // one to spend a request on. Deduplicates identical requests

    const bestResources: (readonly [Algebra.Pattern, IDerivedResource])[] = [];
    const seenRequests = new Set<string>();
    for (const block of this.triplePatternBlocks(action)) {
      const best = block.resources.reduce((min, curr) => this.cost(curr) < this.cost(min) ? curr : min);
      const requestUrl = this.requestUrl(block.pattern, best);
      if (requestUrl !== undefined) {
        if (seenRequests.has(requestUrl)) {
          continue;
        }
        seenRequests.add(requestUrl);
      }
      bestResources.push(<const> [ block.pattern, best ]);
    }

    // Tell link traversal not to crawl what these resources already cover. This is only sound
    // because the proposer refuses to offer anything unless every pattern is covered
    const dynamicLinkFilter = context.getSafe(KeysRdfResolveHypermediaLinks.dynamicFilter);
    for (const [ , resource ] of bestResources) {
      for (const selector of resource.selectors) {
        if (this.isGlob(selector)) {
          dynamicLinkFilter.addGlob(selector);
        } else {
          dynamicLinkFilter.addExact(selector);
        }
      }
    }

    // Fast path: if the query contains a ?s ?p ?o
    // (e.g. wildcard or NPS), all data is fetched in one request
    if (hasWildcardQueryPattern) {
      const wildcardEntry = bestResources.find(([ pattern ]) => this.isWildcardPattern(pattern));

      if (wildcardEntry) {
        const [ wildcardPattern, resource ] = wildcardEntry;
        const rawQuads = resource.querySource.queryQuads(wildcardPattern, context);

        const rdfMetadataOutput: IActorRdfMetadataOutput = await this.mediatorMetadata.mediate(
          { context, url: resource.iri, quads: rawQuads },
        );

        const extractedLinks = await this.mediatorExtractLinks.mediate({
          context,
          url: resource.iri,
          metadata: rdfMetadataOutput.metadata,
          requestTime: 0,
        });

        let dataToImport = rdfMetadataOutput.data;
        await this.importIntoStore(manager, dataToImport, resource.baseUrl);

        return { links: this.dedupeLinks(extractedLinks.links) };
      }
    }

    const answeredQueryPatterns = new Set<Algebra.Pattern>();

    const runEntry = async([ pattern, resource ]: readonly [Algebra.Pattern, IDerivedResource]) => {
      const rawQuads = resource.querySource.queryQuads(pattern, context);

      // We always need to extract links of each of the patterns
      const rdfMetadataOutput: IActorRdfMetadataOutput = await this.mediatorMetadata.mediate(
        { context, url: resource.iri, quads: rawQuads },
      );

      // Left unawaited so link extraction runs alongside the import below
      const extractedLinks = this.mediatorExtractLinks.mediate({
        context,
        url: resource.iri,
        metadata: rdfMetadataOutput.metadata,
        requestTime: 0,
      });

      let dataToImport = this.filterDataToImport(
        pattern,
        rdfMetadataOutput.data,
        queryPatterns,
        answeredQueryPatterns,
        queryPattern => this.claimedByAcceptedComposite(action, queryPattern, resource.baseUrl, queryPatterns),
      );

      if (dataToImport) {
        await this.importIntoStore(manager, dataToImport, resource.baseUrl);
      }

      return extractedLinks;
    };

    // Patterns served by the same resource that can batch go out in one request: link extraction then
    // sees all of their data at once, and only what answers a query pattern is imported
    const runBatch = async(resource: IDerivedResource, patterns: Algebra.Pattern[]): Promise<IActorExtractLinksOutput> => {
      const source = <IBatchQuerySource> resource.querySource;
      const rdfMetadataOutput: IActorRdfMetadataOutput = await this.mediatorMetadata.mediate(
        { context, url: resource.iri, quads: source.queryQuadsBatch(patterns, context) },
      );
      const extractedLinks = this.mediatorExtractLinks.mediate({
        context,
        url: resource.iri,
        metadata: rdfMetadataOutput.metadata,
        requestTime: 0,
      });

      let dataToImport = this.filterBatchDataToImport(
        patterns,
        rdfMetadataOutput.data,
        queryPatterns,
        answeredQueryPatterns,
        queryPattern => this.claimedByAcceptedComposite(action, queryPattern, resource.baseUrl, queryPatterns),
      );
      if (dataToImport) {
        await this.importIntoStore(manager, dataToImport, resource.baseUrl);
      }
      return extractedLinks;
    };

    // The patterns already asked of each batch resource, so a later batch leaves out what they cover
    const requested = new Map<IDerivedResource, Algebra.Pattern[]>();
    const runGroup = (entries: (readonly [Algebra.Pattern, IDerivedResource])[]): Promise<IActorExtractLinksOutput>[] => {
      const batches = new Map<IDerivedResource, Algebra.Pattern[]>();
      const single: (readonly [Algebra.Pattern, IDerivedResource])[] = [];
      for (const entry of entries) {
        if (isBatchQuerySource(entry[1].querySource)) {
          batches.set(entry[1], [ ...batches.get(entry[1]) ?? [], entry[0] ]);
        } else {
          single.push(entry);
        }
      }
      const runs = single.map(runEntry);
      for (const [ resource, asked ] of batches) {
        // A batch answers every pattern in it separately, so a pattern another one covers is sent back
        // twice. Its quads are in the answer to the wider pattern already, and the data is filtered to
        // the query patterns it answers before it is imported, so it can be left out
        const before = requested.get(resource) ?? [];
        const patterns = this.withoutCovered(asked, before);
        requested.set(resource, [ ...before, ...patterns ]);
        if (patterns.length === 0) {
          continue;
        }
        const { maxBatchSize } = <IBatchQuerySource> resource.querySource;
        for (let start = 0; start < patterns.length; start += maxBatchSize) {
          runs.push(runBatch(resource, patterns.slice(start, start + maxBatchSize)));
        }
      }
      return runs;
    };

    // A request that answers a query pattern no composite answers is what the query waits on, so it
    // goes now. The rest, completing patterns a composite already answers or only there to find
    // links, waits until the composites are answered: the server evaluates everything sent to it at
    // once, and next to these pod-wide requests a composite is answered many times slower
    const answeredByComposite = new Set(this.compositeBlocks(action)
      .flatMap(block => block.operations.flatMap(operation => this.extractPatternsQuery(operation)))
      .map(pattern => this.termsKey(pattern)));
    const openQueryPatterns = queryPatterns.filter(pattern => !answeredByComposite.has(this.termsKey(pattern)));
    const neededNow = ([ pattern ]: readonly [Algebra.Pattern, IDerivedResource]): boolean =>
      openQueryPatterns.some(queryPattern => matchPatternMappings(queryPattern, pattern, { skipVarMapping: true }));

    const deferred = bestResources.filter(entry => !neededNow(entry));
    const extractLinksOutput = await Promise.all([
      ...runGroup(bestResources.filter(entry => neededNow(entry))),
      ...deferred.length === 0 ? [] : [
        (action.compositeResponses?.wait() ?? Promise.resolve())
          .then(() => Promise.all(runGroup(deferred)))
          .then(outputs => ({ links: outputs.flatMap(output => output.links) })),
      ],
    ]);

    return { links: this.dedupeLinks(extractLinksOutput.flatMap(output => output.links)) };
  }

  /**
   * Quads that a composite resource completely covers. Drops the bindings the resource
   * has claimed to prevent duplicate indexing and join work.
  */
  protected claimedByAcceptedComposite(
    action: IActionDerivedResourceExecute,
    pattern: Algebra.Pattern,
    container: string,
    queryPatterns: Algebra.Pattern[],
  ): ((quad: RDF.Quad) => boolean) | undefined {
    const blocks = this.acceptedBlocksWith(action, pattern, container);
    if (!this.answeredByAcceptedComposite(action, blocks, pattern, queryPatterns)) {
      return undefined;
    }

    const domainIsDelimited = isDomainDelimited(container);
    const anchorPositions = new Set<QuadTermName>();
    for (const block of blocks) {
      if (block.anchorTerms.length === 0) {
        return undefined;
      }
      for (const anchor of block.anchorTerms) {
        if (anchor.termType === 'Variable') {
          // A variable repeated in the pattern binds the same term at each of its positions
          const [ position ] = filterQuadTermNames(pattern, term => term.equals(anchor));
          if (!position) {
            return undefined;
          }
          anchorPositions.add(position);
        } else if (anchor.termType !== 'NamedNode' || !isWithinDomain(anchor.value, container, domainIsDelimited)) {
          // The join only filters for a block whose constant anchors are within the domain
          return undefined;
        }
      }
    }

    const positions = [ ...anchorPositions ];
    return quad => positions.every(position => quad[position].termType === 'BlankNode' ||
      isWithinDomain(quad[position].value, container, domainIsDelimited));
  }

  /**
   * The composites the join took on for the given container that hold this pattern. Only known for
   * certain once the composite response gate is closed, though a block once accepted stays accepted.
   */
  protected acceptedBlocksWith(
    action: IActionDerivedResourceExecute,
    pattern: Algebra.Pattern,
    container: string,
  ): ICompositeExecutionBlock[] {
    const key = this.termsKey(pattern);
    return (action.compositeResponses?.acceptedBlocks() ?? []).filter(block =>
      block.resource.baseUrl === container &&
      block.operations.some(operation => this.extractPatternsQuery(operation)
        .some(inner => this.termsKey(inner) === key)));
  }

  /**
   * Whether the given accepted composites answer this pattern wherever the query has it. The quads left
   * out of the store are left out for every join reading it, so each join the pattern occurs in, such
   * as every branch of a union, needs a composite answering it there.
   */
  protected answeredByAcceptedComposite(
    action: IActionDerivedResourceExecute,
    blocks: ICompositeExecutionBlock[],
    pattern: Algebra.Pattern,
    queryPatterns: Algebra.Pattern[],
  ): boolean {
    const key = this.termsKey(pattern);
    const isPattern = (operation: Algebra.Operation): boolean => operation.type === Algebra.Types.PATTERN &&
      this.termsKey(<Algebra.Pattern> operation) === key;

    const controller = action.context.get(KeysRdfJoin.adaptiveJoinController);

    if (blocks.length === 0 || queryPatterns.length === 0 || !controller) {
      return false;
    }

    const occurrences = queryPatterns.filter(isPattern).length;
    const covered = controller.getComponents()
      .filter(component => blocks.some(block => component.canCoverOperations(block.operations)))
      .reduce((count, component) => count + component.operations.filter(isPattern).length, 0);
    return covered >= occurrences;
  }

  /**
   * The given patterns without those whose quads another of them, or one requested before, returns as
   * well. Of two equal patterns the first is kept.
   */
  public withoutCovered(patterns: Algebra.Pattern[], requestedBefore: Algebra.Pattern[]): Algebra.Pattern[] {
    return patterns.filter((pattern, index) =>
      !requestedBefore.some(other => this.covers(other, pattern)) &&
      !patterns.some((other, otherIndex) => otherIndex !== index && this.covers(other, pattern) &&
        (otherIndex < index || !this.covers(pattern, other))));
  }

  /**
   * Whether every quad matching `specific` also matches `general`. A pattern in the default graph is
   * sent without a graph, which the server matches against every graph, so it covers any graph.
   */
  public covers(general: Algebra.Pattern, specific: Algebra.Pattern): boolean {
    // A variable occurring twice asks for equal terms, which this does not compare, so such a pattern
    // is only taken to cover itself
    const variables = filterQuadTermNames(general, term => term.termType === 'Variable')
      .map(name => general[name].value);
    if (new Set(variables).size !== variables.length) {
      return general === specific;
    }
    for (const name of [ 'subject', 'predicate', 'object' ] as const) {
      if (general[name].termType !== 'Variable' && !general[name].equals(specific[name])) {
        return false;
      }
    }
    return general.graph.termType === 'Variable' || general.graph.termType === 'DefaultGraph' ||
      general.graph.equals(specific.graph);
  }

  /**
   * The URL a resource would request for this pattern, or undefined if it cannot say.
   */
  protected requestUrl(pattern: Algebra.Pattern, resource: IDerivedResource): string | undefined {
    const source = <{ getFilledTemplateUri?: (operation: Algebra.Pattern) => string }> resource.querySource;
    if (typeof source.getFilledTemplateUri !== 'function') {
      return undefined;
    }
    return `${resource.iri.length}:${resource.iri}${source.getFilledTemplateUri(pattern)}`;
  }

  /**
   * How much this resource is expected to cost, lower being better.
   */
  protected cost(resource: IDerivedResource): number {
    return resource.resourceCoefficients.compute * this.derivedResourceCoefficients.compute +
      resource.resourceCoefficients.requests * this.derivedResourceCoefficients.requests +
      resource.resourceCoefficients.selectivity * this.derivedResourceCoefficients.selectivity;
  }


  /**
   * Determine the data stream to import into the aggregated store for a given traversal pattern.
   */
  public filterDataToImport(
    pattern: Algebra.Pattern,
    data: RDF.Stream<RDF.Quad>,
    queryPatterns: Algebra.Pattern[],
    answeredQueryPatterns: Set<Algebra.Pattern>,
    claimedBy: (queryPattern: Algebra.Pattern) => ((quad: RDF.Quad) => boolean) | undefined = () => undefined,
  ): RDF.Stream<RDF.Quad> | undefined {
    // Only import when actually in query and not already answered
    const matchingQueryPatterns = queryPatterns.filter(queryPattern =>
      !answeredQueryPatterns.has(queryPattern) &&
      matchPatternMappings(queryPattern, pattern, { skipVarMapping: true }));
    for (const queryPattern of matchingQueryPatterns) {
      answeredQueryPatterns.add(queryPattern);
    }

    if (matchingQueryPatterns.length === 0) {
      return undefined;
    }

    const isSpecialization = matchingQueryPatterns.every(queryPattern =>
      filterQuadTermNames(pattern, term => term.termType === 'Variable')
        .some(name => queryPattern[name].termType !== 'Variable'));
    const claims = matchingQueryPatterns.map(queryPattern => claimedBy(queryPattern));

    if (!isSpecialization && claims.every(claimed => claimed === undefined)) {
      return data;
    }
    return wrap<RDF.Quad>(data).filter(quad => matchingQueryPatterns.some((queryPattern, index) =>
      matchPatternComplete(quad, queryPattern) && !claims[index]?.(quad)));
  }

  /**
   * Filters out data that is not required to answer the query. An example of this
   * are link traversal predicates.
   */
  public filterBatchDataToImport(
    patterns: Algebra.Pattern[],
    data: RDF.Stream<RDF.Quad>,
    queryPatterns: Algebra.Pattern[],
    answeredQueryPatterns: Set<Algebra.Pattern>,
    claimedBy: (queryPattern: Algebra.Pattern) => ((quad: RDF.Quad) => boolean) | undefined = () => undefined,
  ): RDF.Stream<RDF.Quad> | undefined {
    const matchingQueryPatterns = new Set<Algebra.Pattern>();
    for (const pattern of patterns) {
      for (const queryPattern of queryPatterns) {
        if (!answeredQueryPatterns.has(queryPattern) &&
          matchPatternMappings(queryPattern, pattern, { skipVarMapping: true })) {
          matchingQueryPatterns.add(queryPattern);
        }
      }
    }
    for (const queryPattern of matchingQueryPatterns) {
      answeredQueryPatterns.add(queryPattern);
    }
    if (matchingQueryPatterns.size === 0) {
      return undefined;
    }

    const answering = [ ...matchingQueryPatterns ].map(queryPattern => <const> [ queryPattern, claimedBy(queryPattern) ]);
    return wrap<RDF.Quad>(data).filter(quad => answering.some(([ queryPattern, claimed ]) =>
      matchPatternComplete(quad, queryPattern) && !claimed?.(quad)));
  }

  protected dedupeLinks(links: ILink[]): ILink[] {
    const byUrl = new Map<string, ILink>();
    for (const link of links) {
      if (!byUrl.has(link.url)) {
        byUrl.set(link.url, link);
      }
    }
    return [ ...byUrl.values() ];
  }

  /**
   * A pattern that matches everything, and so is answered in a single request.
   */
  protected isWildcardPattern(pattern: Algebra.Pattern): boolean {
    const varNames = filterQuadTermNames(pattern, term => term.termType === 'Variable');
    return varNames.length === 4 || (varNames.length === 3 && pattern.graph.termType !== 'Variable');
  }

  protected async importIntoStore(
    manager: ILinkTraversalManager,
    data: RDF.Stream<RDF.Quad>,
    source?: string,
  ): Promise<void> {
    const eventEmitter = manager.getAggregatedStore().import(data, source);
    await new Promise((resolve, reject) => {
      eventEmitter.on('end', resolve);
      eventEmitter.on('error', reject);
    }).catch(() => {
      throw new Error(`Importing triple pattern derived resource failed`);
    });
  }

  /**
   * Identifies a pattern by its terms, variable names included.
   */
  protected termsKey(pattern: Algebra.Pattern): string {
    return [ pattern.subject, pattern.predicate, pattern.object, pattern.graph ]
      .map(term => `${term.termType}:${term.value}`)
      .join('|');
  }

  /**
   * The patterns of the query itself, with every property path reduced to the patterns it walks.
   */
  protected extractPatternsQuery(operation: Algebra.Operation): Algebra.Pattern[] {
    const patterns: Algebra.Pattern[] = [];
    algebraUtils.visitOperation(operation, {
      [Algebra.Types.PATTERN]: {
        preVisitor: () => ({ continue: false }),
        visitor: (pattern: Algebra.Pattern) => {
          patterns.push(pattern);
        },
      },
      [Algebra.Types.PATH]: {
        preVisitor: () => ({ continue: false }),
        visitor: (path: Algebra.Path) => {
          algebraUtils.visitOperation(path, {
            [Algebra.Types.LINK]: {
              preVisitor: () => ({ continue: false }),
              visitor: (link: Algebra.Link) => {
                patterns.push(this.algebraFactory.createPattern(VAR_SUBJ, link.iri, VAR_OBJ, path.graph));
              },
            },
            [Algebra.Types.NPS]: {
              preVisitor: () => ({ continue: false }),
              visitor: () => {
                patterns.push(this.algebraFactory.createPattern(VAR_SUBJ, VAR_PRED, VAR_OBJ, path.graph));
              },
            },
          });
        },
      },
    });
    return patterns;
  }
}

export interface IActorDerivedResourceExecuteTriplePatternArgs extends IActorDerivedResourceExecuteArgs {
  /**
   * The coefficients used to weigh resources against each other
   */
  derivedResourceCoefficients: IDerivedResourceCoefficients;
  /**
   * The metadata mediator
   */
  mediatorMetadata: MediatorRdfMetadata;
  /**
   * The extract links mediator
   */
  mediatorExtractLinks: MediatorExtractLinks;
  /**
   * The metadata extract mediator
   */
  mediatorMetadataExtract: MediatorRdfMetadataExtract;
}
