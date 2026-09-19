import type { IDerivedResource, IDerivedResourceCoefficients } from '@comunica/actor-extract-links-solid-derived-resources';
import type { IActionDerivedResourceExecute, IActorDerivedResourceExecuteOutput, IActorDerivedResourceExecuteArgs } from '@comunica/bus-derived-resource-execute';
import { ActorDerivedResourceExecute } from '@comunica/bus-derived-resource-execute';
import type { MediatorExtractLinks } from '@comunica/bus-extract-links';
import type { IActorRdfMetadataOutput, MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import type { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import { KeysInitQuery } from '@comunica/context-entries';
import { KeysQuerySourceIdentifyLinkTraversal, KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import type { TestResult, IActorTest } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
import type { ComunicaDataFactory, ILink } from '@comunica/types';
import type { ILinkTraversalManager } from '@comunica/types-link-traversal';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import { wrap } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
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

    const context = action.context;
    const manager = context.getSafe(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager);
    manager.addDereferencingDerivedResource(controller);

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

    const shouldAnnotate = context.get(KeysRdfResolveHypermediaLinks.annotateSources) === 'graph';

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
        if (shouldAnnotate) {
          dataToImport = this.annotateQuadsWithSource(dataToImport, resource.baseUrl);
        }

        await this.importIntoStore(manager, dataToImport, resource.baseUrl);

        manager.removeDereferencingDerivedResource(controller);
        return { links: this.dedupeLinks(extractedLinks.links) };
      }
    }

    const answeredQueryPatterns = new Set<Algebra.Pattern>();

    const extractLinksOutput = await Promise.all(
      bestResources.map(async([ pattern, resource ]) => {
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
        );

        if (dataToImport) {
          if (shouldAnnotate) {
            dataToImport = this.annotateQuadsWithSource(dataToImport, resource.baseUrl);
          }
          await this.importIntoStore(manager, dataToImport, resource.baseUrl);
        }

        return extractedLinks;
      }),
    );

    manager.removeDereferencingDerivedResource(controller);
    return { links: this.dedupeLinks(extractLinksOutput.flatMap(output => output.links)) };
  }

  /**
   * The links by url, keeping the first of each. Links are objects, so a set would compare them by
   * reference and never drop anything.
   */
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
   * A pattern that matches everything, and so is answered in a single request.
   */
  protected isWildcardPattern(pattern: Algebra.Pattern): boolean {
    const varNames = filterQuadTermNames(pattern, term => term.termType === 'Variable');
    return varNames.length === 4 || (varNames.length === 3 && pattern.graph.termType !== 'Variable');
  }

  /**
   * Imports the given quads into the aggregated store.
   *
   * A derived resource aggregates documents of the pod it lives on, so the URL it was requested
   * under names a subtree rather than a document. The store keeps whichever of the two names is
   * narrower where traversal also reaches one of those documents itself.
   * @param manager The link traversal manager holding the store.
   * @param data The quads to import.
   * @param source The URL the data was requested under.
   */
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
   * Determine the data stream to import into the aggregated store for a given traversal pattern.
   * Filters out quads if the query patterns are specializations of the traversal pattern,
   * or returns undefined if the pattern is solely for traversal or already answered.
   */
  public filterDataToImport(
    pattern: Algebra.Pattern,
    data: RDF.Stream<RDF.Quad>,
    queryPatterns: Algebra.Pattern[],
    answeredQueryPatterns: Set<Algebra.Pattern>,
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

    return isSpecialization ?
      wrap<RDF.Quad>(data).filter(quad =>
        matchingQueryPatterns.some(queryPattern => matchPatternComplete(quad, queryPattern))) :
      data;
  }

  protected annotateQuadsWithSource(
    data: RDF.Stream<RDF.Quad>,
    sourceIri: string,
  ): RDF.Stream<RDF.Quad> {
    const sourceNode = this.dataFactory.namedNode(sourceIri);
    return wrap<RDF.Quad>(data).map((quad) => {
      if (quad.graph.termType === 'DefaultGraph') {
        return this.dataFactory.quad(quad.subject, quad.predicate, quad.object, sourceNode);
      }
      return quad;
    });
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
