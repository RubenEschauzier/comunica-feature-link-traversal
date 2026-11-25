import { Algebra } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';

/**
 * The FILTERs of a query that may be added to the query a composite resource is asked for a block.
 *
 * A filter F whose variables all occur in block B, and which applies to every solution the block's patterns take
 * part in, commutes with the join: σ_F(B ⋈ R) = σ_F(B) ⋈ R. It also distributes over the disjoint decomposition of B
 * into its delegated part B(A) and the rest, so the resource may answer σ_F(B(A)) in place of B(A). The default plan
 * keeps pruning on the anchors alone: a pruned mapping failing F would have been discarded by F anyway, as the query's
 * own filter stays in the plan. Pushdown therefore changes what the resource sends, never which solutions result.
 *
 * Conjunctions are split, and each conjunct is taken on its own: if a conjunct is false or an error, the whole
 * conjunction is false or an error, and the solution is discarded either way.
 *
 * A filter applies to every solution of a pattern when the pattern occurs only below it, reached through operators
 * that pass every solution of their input on unchanged or not at all (joins, unions, the left side of an OPTIONAL or
 * MINUS, DISTINCT, REDUCED, ORDER BY, BIND). Every other operator (the optional side of an OPTIONAL, the right side of
 * a MINUS, sub-queries, GRAPH, SERVICE, LIMIT, grouping) starts a new scope. A pattern occurring several times must be
 * below the filter at every occurrence, since the resource's answer stands in for each.
 */
export class FilterPushdown {
  /**
   * The conjuncts of the query's filters that may be pushed into a resource's query for a block of these patterns.
   */
  public static pushableFilters(query: Algebra.Operation, patterns: Algebra.Pattern[]): Algebra.Expression[] {
    if (patterns.length === 0) {
      return [];
    }
    const keys = new Set(patterns.map(pattern => FilterPushdown.patternKey(pattern)));
    const blockVariables = new Set<string>();
    for (const pattern of patterns) {
      for (const term of [ pattern.subject, pattern.predicate, pattern.object, pattern.graph ]) {
        if (term.termType === 'Variable') {
          blockVariables.add(term.value);
        }
      }
    }

    // The conjuncts in scope at every occurrence of every block pattern
    let common: Algebra.Expression[] | undefined;
    const found = new Set<string>();
    const visit = (operation: Algebra.Operation, scope: Algebra.Expression[]): void => {
      switch (operation.type) {
        case Algebra.Types.PATTERN: {
          const key = FilterPushdown.patternKey(<Algebra.Pattern> operation);
          if (keys.has(key)) {
            found.add(key);
            common = common === undefined ? [ ...scope ] : common.filter(expression => scope.includes(expression));
          }
          return;
        }
        case Algebra.Types.BGP:
          for (const pattern of (<Algebra.Bgp> operation).patterns) {
            visit(pattern, scope);
          }
          return;
        case Algebra.Types.FILTER: {
          const filter = <Algebra.Filter> operation;
          visit(filter.input, [ ...scope, ...FilterPushdown.conjuncts(filter.expression) ]);
          return;
        }
        case Algebra.Types.JOIN:
        case Algebra.Types.UNION:
          for (const input of (<Algebra.Join | Algebra.Union> operation).input) {
            visit(input, scope);
          }
          return;
        case Algebra.Types.LEFT_JOIN:
        case Algebra.Types.MINUS: {
          const [ left, right ] = (<Algebra.LeftJoin | Algebra.Minus> operation).input;
          visit(left, scope);
          visit(right, []);
          return;
        }
        case Algebra.Types.DISTINCT:
        case Algebra.Types.REDUCED:
        case Algebra.Types.ORDER_BY:
        case Algebra.Types.EXTEND:
          visit((<Algebra.Single> <unknown> operation).input, scope);
          return;
        default:
          // A new scope: what is below is still visited, so a filter inside it is found
          for (const input of FilterPushdown.inputs(operation)) {
            visit(input, []);
          }
      }
    };
    visit(query, []);

    if (common === undefined || found.size < keys.size) {
      // Some block pattern does not occur in the query as such, so where it applies is not known
      return [];
    }
    return common.filter((expression) => {
      const variables = FilterPushdown.expressionVariables(expression);
      return variables !== undefined && variables.size > 0 &&
        [ ...variables ].every(variable => blockVariables.has(variable));
    });
  }

  /**
   * The conjuncts of an expression: the operands of its top-level &&, recursively.
   */
  public static conjuncts(expression: Algebra.Expression): Algebra.Expression[] {
    if (expression.subType === Algebra.ExpressionTypes.OPERATOR &&
      (<Algebra.OperatorExpression> expression).operator === '&&') {
      return (<Algebra.OperatorExpression> expression).args.flatMap(arg => FilterPushdown.conjuncts(arg));
    }
    return [ expression ];
  }

  /**
   * The variables of an expression, or undefined if it holds something an endpoint is not asked to evaluate
   * apart from the query (EXISTS, aggregates, wildcards).
   */
  public static expressionVariables(expression: Algebra.Expression): Set<string> | undefined {
    const variables = new Set<string>();
    const collect = (current: Algebra.Expression): boolean => {
      switch (current.subType) {
        case Algebra.ExpressionTypes.TERM: {
          const term: RDF.Term = (<Algebra.TermExpression> current).term;
          if (term.termType === 'Variable') {
            variables.add(term.value);
          }
          return term.termType !== 'BlankNode';
        }
        case Algebra.ExpressionTypes.OPERATOR:
        case Algebra.ExpressionTypes.NAMED:
          return (<Algebra.OperatorExpression | Algebra.NamedExpression> current).args.every(arg => collect(arg));
        default:
          return false;
      }
    };
    return collect(expression) ? variables : undefined;
  }

  protected static inputs(operation: Algebra.Operation): Algebra.Operation[] {
    const input = (<{ input?: Algebra.Operation | Algebra.Operation[] }> <unknown> operation).input;
    if (Array.isArray(input)) {
      return input;
    }
    return input && typeof input === 'object' && 'type' in input ? [ input ] : [];
  }

  protected static patternKey(pattern: Algebra.Pattern): string {
    return [ pattern.subject, pattern.predicate, pattern.object ]
      .map(term => `${term.termType}|${term.value}`).join(' ');
  }
}
