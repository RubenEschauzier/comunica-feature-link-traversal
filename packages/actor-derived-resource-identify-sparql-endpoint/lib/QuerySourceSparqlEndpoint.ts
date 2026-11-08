import { KeysInitQuery } from '@comunica/context-entries';
import type {
  BindingsStream,
  ComunicaDataFactory,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
} from '@comunica/types';
import type { AlgebraFactory } from '@comunica/utils-algebra';
import { Algebra, isKnownOperation } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';

/**
 * A SPARQL endpoint, as a source for the parts of the query the derived resource pipeline hands it.
 *
 * Comunica's SPARQL source is made to be the source of the whole query, and where the query was
 * written in SPARQL it sends the query string as written rather than the operation it is asked for.
 * Here it is only ever asked for a part of the query, so the query string is kept from it.
 *
 * Every other request is passed on as is, except that a pattern, or a bgp, asked for as quads is
 * asked as the CONSTRUCT returning them: the execute actors ask derived resources for the quads of a
 * pattern to add to the aggregated store, while an endpoint only returns quads for a CONSTRUCT.
 */
export class QuerySourceSparqlEndpoint implements IQuerySource {
  public constructor(
    public readonly referenceValue: string,
    protected readonly source: IQuerySource,
    protected readonly algebraFactory: AlgebraFactory,
    protected readonly dataFactory: ComunicaDataFactory,
  ) {}

  public getSelectorShape(context: IActionContext): Promise<FragmentSelectorShape> {
    return this.source.getSelectorShape(context);
  }

  public getFilterFactor(context: IActionContext): Promise<number> {
    return this.source.getFilterFactor(context);
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    return this.source.queryBindings(operation, this.forPartOfQuery(context), options);
  }

  public queryQuads(operation: Algebra.Operation, context: IActionContext): AsyncIterator<RDF.Quad> {
    return this.source.queryQuads(this.asConstruct(operation), this.forPartOfQuery(context));
  }

  public queryBoolean(operation: Algebra.Ask, context: IActionContext): Promise<boolean> {
    return this.source.queryBoolean(operation, this.forPartOfQuery(context));
  }

  public queryVoid(operation: Algebra.Operation, context: IActionContext): Promise<void> {
    return this.source.queryVoid(operation, this.forPartOfQuery(context));
  }

  public toString(): string {
    return `QuerySourceSparqlEndpoint(${this.referenceValue})`;
  }

  /**
   * The context without the query string, so that the operation asked for is sent rather than the
   * whole query.
   */
  protected forPartOfQuery(context: IActionContext): IActionContext {
    return context.delete(KeysInitQuery.queryString);
  }

  /**
   * The CONSTRUCT returning the quads of a pattern or bgp.
   *
   * A pattern over any graph is asked over the default graph. The quads of a pod are read into the
   * aggregated store as they come, without regard to the graphs of the endpoint, and the default
   * graph is the one an endpoint serves the dataset it was linked from in. An endpoint shared by
   * several datasets, as a single triple store serving each of them on their own endpoint is, holds
   * the others in its named graphs, which a pattern over any graph would read as well.
   */
  protected asConstruct(operation: Algebra.Operation): Algebra.Operation {
    let patterns: Algebra.Pattern[];
    if (isKnownOperation(operation, Algebra.Types.PATTERN)) {
      patterns = [ operation ];
    } else if (isKnownOperation(operation, Algebra.Types.BGP)) {
      patterns = operation.patterns;
    } else {
      return operation;
    }
    patterns = patterns.map(pattern => pattern.graph.termType === 'Variable' ?
      this.algebraFactory.createPattern(pattern.subject, pattern.predicate, pattern.object, this.dataFactory.defaultGraph()) :
      pattern);
    return this.algebraFactory.createConstruct(this.algebraFactory.createBgp(patterns), patterns);
  }
}
