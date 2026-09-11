import { IDerivedResource } from '@comunica/actor-extract-links-solid-derived-resources';
import { ActorDerivedResourcePropose, IActionDerivedResourcePropose, IActorDerivedResourceProposeOutput, IActorDerivedResourceProposeArgs, ICandidateResource } from '@comunica/bus-derived-resource-propose';
import { ActorExtractLinks, IExtractPattern, MediatorExtractLinks } from '@comunica/bus-extract-links';
import { TestResult, IActorTest, passTestVoid } from '@comunica/core';
import { IActionContext } from '@comunica/types';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';
import { doesShapeAcceptOperation } from '@comunica/utils-query-operation';
import { DataFactory } from 'rdf-data-factory';
import type * as RDF from '@rdfjs/types';

const DF = new DataFactory<RDF.BaseQuad>();
const VAR_SUBJ = DF.variable('__comunica:pp_var_subj');
const VAR_PRED = DF.variable('__comunica:pp_var_pred');
const VAR_OBJ = DF.variable('__comunica:pp_var_obj');

/**
 * A comunica Triple Pattern Derived Resource Propose Actor. This proposes a resource for every
 * single pattern a derived resource can serve, so that the data does not have to be crawled for.
 */
export class ActorDerivedResourceProposeTriplePattern extends ActorDerivedResourcePropose {
  protected readonly algebraFactory = new AlgebraFactory();

  public readonly mediatorExtractLinks: MediatorExtractLinks;

  public constructor(args: IActorDerivedResourceProposeTriplePatternArgs) {
    super(args);
    this.mediatorExtractLinks = args.mediatorExtractLinks;
  }

  public async test(action: IActionDerivedResourcePropose): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourcePropose): Promise<IActorDerivedResourceProposeOutput> {
    const candidateResources: ICandidateResource[] = [];
    for (const { pattern, podInternal } of this.patternsToServe(action)) {
      const serving = action.resources.filter(resource =>
        doesShapeAcceptOperation(resource.derivedResourceSelectorShape, pattern));

      // If a triple pattern can't be answered we can't use triple pattern-level derived resources
      // as we need to traverse all files to answer the unanswerable triple pattern
      if (serving.length === 0) {
        return { candidateResources: []};
      }

      for (const resource of serving) {
        // A resource serving a whole pod is not proposed for a pattern whose links stay inside that
        // pod: it already carries the documents those links lead to, so the request buys nothing.
        // The pattern still counts as covered above, this only declines to spend a request on it
        if (podInternal && this.coversWholePod(resource)) {
          continue;
        }
        candidateResources.push(this.createCandidateResource(pattern, resource));
      }
    }
    return { candidateResources };
  }

  /**
   * Gets all triple patterns we need from a derived resource answering a single triple pattern.
   */
  protected patternsToServe(action: IActionDerivedResourcePropose): IExtractPattern[] {
    // The query's own patterns go first, so that one also reported as a pod-internal traversal
    // pattern keeps the query's reading of it: an answer is needed whatever the links do
    const queryPatterns: IExtractPattern[] = this.extractQueryPatterns(action.operation)
      .map(pattern => ({ pattern, podInternal: false }));

    const seen = new Set<string>();
    return [ ...queryPatterns, ...this.extractTraversalPatterns(action.context) ]
      .filter(({ pattern }) => {
        const key = this.patternKey(pattern);
        if (seen.has(key)) {
          return false;
        }
        seen.add(key);
        return true;
      });
  }

  /**
   * The patterns the link extractors would otherwise crawl for, each saying whether its links can
   * leave the pod that served them.
   */
  protected extractTraversalPatterns(context: IActionContext): IExtractPattern[] {
    const actors = <ActorExtractLinks[]> (<any> this.mediatorExtractLinks.bus).actors;
    return actors.flatMap(actor => actor.getExtractPatternRepresentation(context));
  }

  /**
   * Whether the resource serves the whole pod it lives on.
   *
   * Heuristic, pending a way for a resource to state its own scope: a selector that is nothing but
   * a wildcard below the base url covers everything under it.
   */
  protected coversWholePod(resource: IDerivedResource): boolean {
    return resource.selectors.some((selector) => {
      const belowBase = selector.startsWith(resource.baseUrl) ?
        selector.slice(resource.baseUrl.length) :
        selector;
      return [ '*', '**', '/*', '/**' ].includes(belowBase);
    });
  }

  /**
   * The patterns of the query itself, with every property path reduced to the patterns it walks.
   */
  protected extractQueryPatterns(operation: Algebra.Operation): Algebra.Pattern[] {
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

  protected createCandidateResource(
    pattern: Algebra.Pattern,
    resource: IDerivedResource,
  ): ICandidateResource {
    return {
      operations: [ pattern ],
      resource,
      kind: 'triple-pattern',
      // The data is imported into the aggregated store rather than joined against a component, so
      // there is no term the resource has to be authoritative over
      anchorTerms: [],
      prunable: true,
      features: {
        patternCount: 1,
        constantCount: [ pattern.subject, pattern.predicate, pattern.object, pattern.graph ]
          .filter(term => term.termType !== 'Variable').length,
        coefficients: resource.resourceCoefficients,
      },
    };
  }

  protected patternKey(pattern: Algebra.Pattern): string {
    return [ pattern.subject, pattern.predicate, pattern.object, pattern.graph ]
      .map(term => term.termType === 'Variable' ? 'VAR' : `${term.termType}:${term.value}`)
      .join('|');
  }
}

export interface IActorDerivedResourceProposeTriplePatternArgs extends IActorDerivedResourceProposeArgs {
  /**
   * The extract links mediator, whose actors report the patterns they would follow links for
   */
  mediatorExtractLinks: MediatorExtractLinks;
}
