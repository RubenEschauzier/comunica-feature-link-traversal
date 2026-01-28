import { ActionContext } from '@comunica/core';
import type {
  BindingsStream,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
  QuerySourceReference,
} from '@comunica/types';
import { Algebra, isKnownOperation, AlgebraFactory } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import { WrappingIterator, type AsyncIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';

export class QuerySourceCacheWrapper implements IQuerySource {
  private readonly source: IQuerySource;
  private readonly quads: AsyncIterator<RDF.Quad>;

  public referenceValue: QuerySourceReference;

  private readonly sizePromise: Promise<number>;
  private readonly quadCount = 0;

  protected DF: DataFactory = new DataFactory();
  protected AF: AlgebraFactory = new AlgebraFactory(this.DF);

  public constructor(
    source: IQuerySource,
  ) {
    this.quads = new WrappingIterator(source.queryQuads(
      this.AF.createPattern(
        this.DF.variable('s'),
        this.DF.variable('p'),
        this.DF.variable('o'),
        this.DF.variable('g'),
      ),
      new ActionContext(),
    ));
    // Const counterClone = this.quads.clone();
    this.sizePromise = new Promise(resolve => resolve(1));
    // This.sizePromise = new Promise((resolve, reject) => {
    //   // Every time a quad passes through any clone, this listener sees it.
    //   counterClone.on('data', () => {
    //     this.quadCount++;
    //   });

    //   // Resolves the promise when the source stream naturally ends.
    //   counterClone.on('end', () => {
    //     resolve(this.quadCount);
    //   });

    //   counterClone.on('error', (err) => {
    //     reject(err);
    //   });
    // });

    this.source = source;
    this.referenceValue = source.referenceValue;
  }

  public async getSelectorShape(context: IActionContext): Promise<FragmentSelectorShape> {
    return this.source.getSelectorShape(context);
  }

  public async getFilterFactor(context: IActionContext): Promise<number> {
    return this.source.getFilterFactor(context);
  }

  public async getSize(): Promise<number> {
    return this.sizePromise;
  }

  // Public async ingestQuads(): Promise<void>{
  //   const quads = this.source.queryQuads(
  //     this.AF.createPattern(
  //         this.DF.variable('s'),
  //         this.DF.variable('p'),
  //         this.DF.variable('o'),
  //         this.DF.variable('g')
  //     ),
  //     new ActionContext({
  //       [KeysQuerySourceIdentifyHypermediaNoneLazy.nonConsumingQueryQuads.name]: true
  //     })
  //   );
  //   const promiseConsumedSource = new Promise<void>((resolve, reject) => {
  //     quads.on('data', (quad) => this.store.addQuad(quad));
  //     quads.on('end', () => {
  //       resolve();
  //     });
  //     quads.on('error', () => reject("Error importing quads for cached source"));
  //   })
  //   return promiseConsumedSource;
  // }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    return this.source.queryBindings(operation, context, options);
  }

  public queryQuads(
    operation: Algebra.Operation,
    _context: IActionContext,
  ): AsyncIterator<RDF.Quad> {
    if (isKnownOperation(operation, Algebra.Types.PATTERN) &&
        operation.subject.termType === 'Variable' &&
        operation.predicate.termType === 'Variable' &&
        operation.object.termType === 'Variable' &&
        operation.graph.termType === 'Variable') {
      return this.quads.clone();
    }
    throw new Error('queryQuads is not implemented in QuerySourceCacheWrapper');
  }

  public queryBoolean(
    operation: Algebra.Ask,
    context: IActionContext,
  ): Promise<boolean> {
    return this.source.queryBoolean(operation, context);
  }

  public queryVoid(
    operation: Algebra.Operation,
    context: IActionContext,
  ): Promise<void> {
    return this.source.queryVoid(operation, context);
  }

  public toString(): string {
    return `QuerySourceCacheWrapper(${this.source.toString})`;
  }
}
