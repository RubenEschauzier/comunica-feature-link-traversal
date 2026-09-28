import type {
  BindingsStream,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
  QuerySourceReference,
} from '@comunica/types';
import type { Algebra } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { wrap } from 'asynciterator';

/**
 * A query source that is only created once it is first queried.
 *
 * Identifying a derived resource only needs to know what it can answer, which for some kinds is
 * known up front. Creating the source itself can take a request, and one made while identifying
 * holds up every derived resource of the pod, including those that do not need it.
 */
export class LazyQuerySource implements IQuerySource {
  private source: Promise<IQuerySource> | undefined;

  public constructor(
    public readonly referenceValue: QuerySourceReference,
    protected readonly selectorShape: FragmentSelectorShape,
    protected readonly createSource: () => Promise<IQuerySource>,
  ) {}

  protected getSource(): Promise<IQuerySource> {
    if (!this.source) {
      this.source = this.createSource();
    }
    return this.source;
  }

  public async getSelectorShape(_context: IActionContext): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public async getFilterFactor(context: IActionContext): Promise<number> {
    return (await this.getSource()).getFilterFactor(context);
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    return <BindingsStream> this.wrapWithMetadata(
      this.getSource().then(source => source.queryBindings(operation, context, options)),
    );
  }

  public queryQuads(operation: Algebra.Operation, context: IActionContext): AsyncIterator<RDF.Quad> {
    return this.wrapWithMetadata(this.getSource().then(source => source.queryQuads(operation, context)));
  }

  public async queryBoolean(operation: Algebra.Ask, context: IActionContext): Promise<boolean> {
    return (await this.getSource()).queryBoolean(operation, context);
  }

  public async queryVoid(operation: Algebra.Operation, context: IActionContext): Promise<void> {
    return (await this.getSource()).queryVoid(operation, context);
  }

  public toString(): string {
    return `LazyQuerySource(${typeof this.referenceValue === 'string' ? this.referenceValue : 'source'})`;
  }

  /**
   * A stream over the one the source will produce, passing on its metadata once it is known.
   */
  protected wrapWithMetadata<T>(inner: Promise<AsyncIterator<T>>): AsyncIterator<T> {
    const outer = wrap<T>(inner);
    inner
      .then(stream => stream.getProperty('metadata', metadata => outer.setProperty('metadata', metadata)))
      // A failing source errors the stream through wrap already
      .catch(() => {});
    return outer;
  }
}
