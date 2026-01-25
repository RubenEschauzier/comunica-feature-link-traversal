import { KeysQuerySourceIdentifyHypermediaNoneLazy } from '@comunica/context-entries-link-traversal';
import type {
  BindingsStream,
  ComunicaDataFactory,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
  QuerySourceReference,
} from '@comunica/types';
import { Algebra, isKnownOperation, AlgebraFactory } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import { AsyncIterator, WrappingIterator } from 'asynciterator';
import { TransformIterator } from 'asynciterator';

export class QuerySourceFileLazy implements IQuerySource {
  private quads: RDF.Stream | undefined;
  private originalClonedQuads: AsyncIterator<RDF.Quad> | undefined;
  private source: Promise<IQuerySource> | undefined;
  protected readonly selectorShape: FragmentSelectorShape;
  public referenceValue: QuerySourceReference;

  public constructor(
    quads: RDF.Stream,
    protected readonly dataFactory: ComunicaDataFactory,
    protected readonly url: string,
    protected readonly sourceIndexer: (quads: RDF.Stream) => Promise<IQuerySource>,
  ) {
    this.quads = quads;
    this.url = url;
    this.referenceValue = url;
    this.dataFactory = dataFactory;
    const AF = new AlgebraFactory(this.dataFactory);
    this.selectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern: AF.createPattern(
          this.dataFactory.variable('s'),
          this.dataFactory.variable('p'),
          this.dataFactory.variable('o'),
        ),
      },
      variablesOptional: [
        this.dataFactory.variable('s'),
        this.dataFactory.variable('p'),
        this.dataFactory.variable('o'),
      ],
    };
  }

  public async getSelectorShape(): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public async getFilterFactor(): Promise<number> {
    return 0;
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    if (!this.quads && !this.source) {
      throw new Error('Illegal invocation of queryBindings, as the lazy quads stream has already been consumed.');
    }
    this.source = this.source ?? this.sourceIndexer(this.quads!);
    this.quads = undefined;
    const sourceThis = this.source;
    const it = new TransformIterator(async() => {
      const source = await sourceThis;
      const bindings = source.queryBindings(operation, context, options);
      bindings.getProperty('metadata', metadata => it.setProperty('metadata', metadata));
      return bindings;
    });
    return it;
  }

  public queryQuads(
    operation: Algebra.Operation,
    context: IActionContext,
  ): AsyncIterator<RDF.Quad> {
    if (!this.quads) {
      throw new Error('Illegal invocation of queryQuads, as the lazy quads stream has already been consumed.');
    }
    if (isKnownOperation(operation, Algebra.Types.PATTERN) &&
        operation.subject.termType === 'Variable' &&
        operation.predicate.termType === 'Variable' &&
        operation.object.termType === 'Variable' &&
        operation.graph.termType === 'Variable') {
      // Check if context says it is a non-consuming call. This can be when a cache
      // wants to import the data into its cache before the AggregatedStore imports 
      // the quads
      if (context.get(KeysQuerySourceIdentifyHypermediaNoneLazy.nonConsumingQueryQuads)){
        this.originalClonedQuads = new WrappingIterator(this.quads);
        return this.originalClonedQuads.clone();
      }
      // If this source has been cloned we only return the clone
      if (this.originalClonedQuads){
        const ret = this.originalClonedQuads.clone();
        this.quads = undefined;
        this.originalClonedQuads = undefined;
        return ret;
      }
      const ret = this.quads;
      this.quads = undefined;
      // This does not return an AsyncIterator to save some CPU cycles,
      // as the aggregated store can handle regular streams.
      return <any> ret;
    }
    throw new Error('queryQuads is not implemented in QuerySourceFileLazy');
  }

  public queryBoolean(
    _operation: Algebra.Ask,
    _context: IActionContext,
  ): Promise<boolean> {
    throw new Error('queryBoolean is not implemented in QuerySourceFileLazy');
  }

  public queryVoid(
    _operation: Algebra.Operation,
    _context: IActionContext,
  ): Promise<void> {
    throw new Error('queryVoid is not implemented in QuerySourceFileLazy');
  }

  public toString(): string {
    return `QuerySourceFileLazy(${this.url})`;
  }
}
