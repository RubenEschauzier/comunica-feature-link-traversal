import type { MediatorExtractLinks } from '@comunica/bus-extract-links';
import { ActorExtractLinks } from '@comunica/bus-extract-links';
import type {
  BindingsStream,
  ComunicaDataFactory,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
  QuerySourceReference,
} from '@comunica/types';
import type { ILinkTraversalManager } from '@comunica/types-link-traversal';
import { Algebra, algebraUtils } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { MappingIterator } from 'asynciterator';
import { PassThrough } from 'readable-stream';

/**
 * A mapping iterator that calls back once it closes, whether it ends or is destroyed.
 */
class ClosingMappingIterator<T> extends MappingIterator<T> {
  public constructor(source: AsyncIterator<T>, map: (item: T) => T, private readonly onClose: () => void) {
    super(source, map);
  }

  protected override _end(destroy: boolean): void {
    super._end(destroy);
    this.onClose();
  }
}

/**
 * A query source that extracts links from the answers of another query source, as link traversal extracts
 * them from documents, and hands them to the link traversal manager.
 *
 * The bindings are passed on unchanged. For every binding, the triples it matched in the triple patterns of
 * the operation are rebuilt and streamed into the extract links mediator, under the url of the source. Until
 * that stream has ended and its links are handed over, the source is registered as work in progress with the
 * manager, so traversal does not end before these links can be followed.
 */
export class QuerySourceExtractLinks implements IQuerySource {
  public readonly referenceValue: QuerySourceReference;

  public constructor(
    public readonly innerSource: IQuerySource,
    private readonly url: string,
    private readonly mediatorExtractLinks: MediatorExtractLinks,
    private readonly linkTraversalManager: ILinkTraversalManager,
    private readonly dataFactory: ComunicaDataFactory,
  ) {
    this.referenceValue = innerSource.referenceValue;
  }

  public async getFilterFactor(context: IActionContext): Promise<number> {
    return this.innerSource.getFilterFactor(context);
  }

  public async getSelectorShape(context: IActionContext): Promise<FragmentSelectorShape> {
    return this.innerSource.getSelectorShape(context);
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    const bindingsStream = this.innerSource.queryBindings(operation, context, options);
    const patterns = QuerySourceExtractLinks.patterns(operation);
    if (patterns.length === 0) {
      return bindingsStream;
    }

    const quads: RDF.Stream & PassThrough = new PassThrough({ objectMode: true });
    const output = new ClosingMappingIterator(bindingsStream, (bindings) => {
      for (const quad of this.matchedQuads(patterns, bindings)) {
        quads.write(quad);
      }
      return bindings;
    }, () => quads.end());
    output.copyProperties(bindingsStream, [ 'metadata' ]);

    const controller = new AbortController();
    this.linkTraversalManager.addDereferencingDerivedResource(controller);
    ActorExtractLinks.allowSharedMetadataListeners(quads);
    this.mediatorExtractLinks.mediate({ url: this.url, metadata: quads, requestTime: 0, context })
      .then(async({ links }) => {
        if (!controller.signal.aborted) {
          await this.linkTraversalManager.addLinks({ url: this.url }, links);
        }
      })
      // Like the links of a document that fail to be extracted, these do not fail the query
      .catch(() => {})
      .finally(() => {
        // Completing work before traversal has started would make the manager end traversal
        if (this.linkTraversalManager.started) {
          this.linkTraversalManager.completeDereferencingDerivedResource(controller);
        } else {
          this.linkTraversalManager.removeDereferencingDerivedResource(controller);
        }
      });

    return output;
  }

  public queryQuads(operation: Algebra.Operation, context: IActionContext): AsyncIterator<RDF.Quad> {
    return this.innerSource.queryQuads(operation, context);
  }

  public async queryBoolean(operation: Algebra.Ask, context: IActionContext): Promise<boolean> {
    return this.innerSource.queryBoolean(operation, context);
  }

  public async queryVoid(operation: Algebra.Operation, context: IActionContext): Promise<void> {
    return this.innerSource.queryVoid(operation, context);
  }

  public toString(): string {
    return `QuerySourceExtractLinks(${this.innerSource.toString()})`;
  }

  /**
   * The triple patterns in an operation.
   */
  public static patterns(operation: Algebra.Operation): Algebra.Pattern[] {
    const patterns: Algebra.Pattern[] = [];
    algebraUtils.visitOperation(operation, {
      [Algebra.Types.PATTERN]: { visitor: (pattern) => {
        patterns.push(pattern);
      } },
    });
    return patterns;
  }

  /**
   * The triples a binding matched in the given patterns: those of which the binding binds every variable.
   */
  public matchedQuads(patterns: Algebra.Pattern[], bindings: RDF.Bindings): RDF.Quad[] {
    const quads: RDF.Quad[] = [];
    for (const pattern of patterns) {
      const subject = QuerySourceExtractLinks.bind(pattern.subject, bindings);
      const predicate = QuerySourceExtractLinks.bind(pattern.predicate, bindings);
      const object = QuerySourceExtractLinks.bind(pattern.object, bindings);
      const graph = QuerySourceExtractLinks.bind(pattern.graph, bindings) ?? this.dataFactory.defaultGraph();
      if (subject && predicate && object) {
        quads.push(this.dataFactory.quad(
          <RDF.Quad_Subject> subject,
          <RDF.Quad_Predicate> predicate,
          <RDF.Quad_Object> object,
          <RDF.Quad_Graph> graph,
        ));
      }
    }
    return quads;
  }

  private static bind(term: RDF.Term, bindings: RDF.Bindings): RDF.Term | undefined {
    return term.termType === 'Variable' ? bindings.get(term) : term;
  }
}
