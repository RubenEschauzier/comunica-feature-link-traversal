import type { IActionExtractLinks, IActorExtractLinksOutput } from '@comunica/bus-extract-links';
import { ActorExtractLinks } from '@comunica/bus-extract-links';
import { KeysInitQuery } from '@comunica/context-entries';
import type { IActorArgs, IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';
import { Pattern } from '@comunica/utils-algebra/lib/Algebra';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type { QuadTermName } from 'rdf-terms';
import { filterQuadTermNames, getNamedNodes, getTerms, matchPatternComplete } from 'rdf-terms';

const DF = new DataFactory<RDF.BaseQuad>();
const AF = new AlgebraFactory(DF);
const VAR = DF.variable('__comunica:pp_var');
const VAR_SUBJ = DF.variable('__comunica:pp_var_subj');
const VAR_PRED = DF.variable('__comunica:pp_var_pred');
const VAR_OBJ = DF.variable('__comunica:pp_var_obj');
const VAR_GRAPH = DF.variable('__comunica:pp_var_graph')

/**
 * A comunica Traverse Quad Pattern Query RDF Metadata Extract Actor.
 */
export class ActorExtractLinksQuadPatternQuery extends ActorExtractLinks {
  private readonly onlyVariables: boolean;

  public constructor(args: IActorExtractLinksQuadPatternQueryArgs) {
    super(args);
    this.onlyVariables = args.onlyVariables;
  }

  public static getCurrentQuery(context: IActionContext): Algebra.Operation | undefined {
    const currentQueryOperation: Algebra.Operation | undefined = context.get(KeysInitQuery.query);
    if (!currentQueryOperation) {
      return;
    }
    return currentQueryOperation;
  }

  public static matchQuadPatternInOperation(quad: RDF.Quad, operation: Algebra.Operation): RDF.BaseQuad[] {
    const matchingPatterns: RDF.BaseQuad[] = [];
    algebraUtils.visitOperation(operation, {
      [Algebra.Types.PATTERN]: {
        preVisitor: () => ({ continue: false }),
        visitor: (pattern) => {
          if (matchPatternComplete(quad, pattern)) {
            matchingPatterns.push(pattern);
          }
        },
      },
      [Algebra.Types.PATH]: {
        preVisitor: () => ({ continue: false }),
        visitor: (path: Algebra.Path) => {
          algebraUtils.visitOperation(path, {
            [Algebra.Types.LINK]: {
              preVisitor: () => ({ continue: false }),
              visitor: (link: Algebra.Link) => {
                const pattern = DF.quad(VAR, link.iri, VAR, path.graph);
                if (matchPatternComplete(quad, pattern)) {
                  matchingPatterns.push(pattern);
                }
              },
            },
            [Algebra.Types.NPS]: {
              preVisitor: () => ({ continue: false }),
              visitor: (nps: Algebra.Nps) => {
                for (const iri of nps.iris) {
                  const pattern = DF.quad(VAR, iri, VAR, path.graph);
                  if (matchPatternComplete(quad, pattern)) {
                    matchingPatterns.push(pattern);
                  }
                }
              },
            },
          });
        },
      },
    });
    return matchingPatterns;
  }

  public async test(action: IActionExtractLinks): Promise<TestResult<IActorTest>> {
    if (!ActorExtractLinksQuadPatternQuery.getCurrentQuery(action.context)) {
      return failTest(`Actor ${this.name} can only work in the context of a query.`);
    }
    return passTestVoid();
  }

  public async run(action: IActionExtractLinks): Promise<IActorExtractLinksOutput> {
    const operation: Algebra.Operation = ActorExtractLinksQuadPatternQuery
      .getCurrentQuery(action.context)!;

    return {
      links: await ActorExtractLinks.collectStream(action.metadata, (quad, links) => {
        const matchingPatterns = ActorExtractLinksQuadPatternQuery
          .matchQuadPatternInOperation(quad, operation);
        if (matchingPatterns.length > 0) {
          if (this.onlyVariables) {
            // --- If we only want to follow links matching with a variable component ---

            // Determine quad term names that we should check
            const quadTermNames: Partial<Record<QuadTermName, boolean>> = {};
            for (const quadPattern of matchingPatterns) {
              for (const quadTermName of filterQuadTermNames(quadPattern, value => value.termType === 'Variable')) {
                quadTermNames[quadTermName] = true;
              }
            }

            // For the discovered quad term names, check extract the named nodes in the quad
            for (const quadTermName of <QuadTermName[]>Object.keys(quadTermNames)) {
              if (quad[quadTermName].termType === 'NamedNode') {
                links.push({
                  url: quad[quadTermName].value,
                  metadata: { producedByActor: { name: this.name, onlyVariables: this.onlyVariables }},
                });
              }
            }
          } else {
            // --- If we want to follow links, irrespective of matching with a variable component ---
            for (const link of getNamedNodes(getTerms(quad))) {
              links.push({
                url: link.value,
                metadata: { producedByActor: { name: this.name, onlyVariables: this.onlyVariables }},
              });
            }
          }
        }
      }),
    };
  }
  
  public extractPatternsQuery(operation: Algebra.Operation) {
    const patternQuery: Algebra.Pattern[] = [];

    // Recursively extract all LINK/NPS leaves from any path expression,
    // Falls back to a wildcard quad pattern if an unrecognised node type is encountered.
    const extractPathPatterns = (
      node: Algebra.Operation,
      graph: RDF.Term,
      out: Algebra.Pattern[],
    ): void => {
      switch (node.type) {
        case Algebra.Types.LINK: {
          const link = node as Algebra.Link;
          out.push(AF.createPattern(VAR_SUBJ, link.iri, VAR_OBJ, VAR_GRAPH));
          break;
        }
        case Algebra.Types.NPS: {
          // NPS matches any arc *except* these IRIs.
          // We emit a pattern per listed IRI (same as before) so callers can
          // filter them out, but also a wildcard quad to cover the "everything
          // else" side of the negation.
          out.push(AF.createPattern(VAR_SUBJ, VAR_PRED, VAR_OBJ, VAR_GRAPH));
          break;
        }

        // Unary wrappers (INV, *, +, ?) 
        case Algebra.Types.ZERO_OR_MORE_PATH:
        case Algebra.Types.ONE_OR_MORE_PATH: 
        case Algebra.Types.ZERO_OR_ONE_PATH:
        case Algebra.Types.INV: {
          const path = <
            Algebra.Inv | 
            Algebra.ZeroOrMorePath | 
            Algebra.OneOrMorePath |
            Algebra.ZeroOrOnePath> node;
          extractPathPatterns(path.path, graph, out);
          break;
        }
        case Algebra.Types.ALT:
        case Algebra.Types.SEQ: {
          const seq = <Algebra.Seq | Algebra.Alt> node;
          for (const input of seq.input){
            extractPathPatterns(input, graph, out);
          }
          break;
        }
        default: {
          // Unknown or future path node type: emit a wildcard quad pattern so
          // the caller fetches everything in the graph rather than silently
          // missing triples.
          out.push(AF.createPattern(VAR_SUBJ, VAR_PRED, VAR_OBJ, VAR_GRAPH));
          break;
        }
      }
    };

    algebraUtils.visitOperation(operation, {
      [Algebra.Types.PATTERN]: {
        preVisitor: () => ({ continue: false }),
        visitor: (pattern: Algebra.Pattern) => {
          patternQuery.push(
            AF.createPattern(VAR_SUBJ, pattern.predicate, VAR_OBJ, VAR_GRAPH)
          );        
        },
      },
      [Algebra.Types.PATH]: {
        preVisitor: () => ({ continue: false }),
        visitor: (path: Algebra.Path) => {
          extractPathPatterns(path.predicate, path.graph, patternQuery);
        },
      },
    });

    return patternQuery;
  }
  /**
   * 
   * @param context 
   * @returns 
   */
  public getExtractPatternRepresentation(context: IActionContext): Algebra.Pattern[] {
    const query = ActorExtractLinksQuadPatternQuery.getCurrentQuery(context);
    if (!query){
      return [];
    }
    return this.extractPatternsQuery(query);
  }
}

export interface IActorExtractLinksQuadPatternQueryArgs
  extends IActorArgs<IActionExtractLinks, IActorTest, IActorExtractLinksOutput> {
  /**
   * If only links that match a variable in the query should be included.
   * @default {true}
   */
  onlyVariables: boolean;
}
