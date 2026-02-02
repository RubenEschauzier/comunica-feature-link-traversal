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


  protected DF: DataFactory = new DataFactory();
  protected AF: AlgebraFactory = new AlgebraFactory(this.DF);

  private readonly sizePromise: Promise<number>;
  private resolveSize!: (count: number) => void;
  private rejectSize!: (error: Error) => void;

  private countWrapDone = false;
  private quadCount = 0;

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
    this.sizePromise = new Promise<number>((resolve, reject) => {
      this.resolveSize = resolve;
      this.rejectSize = reject;
    });
    
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
      const result = this.quads.clone();
      if (!this.countWrapDone) {
        this.countWrapDone = true;

        result.on('end', () => {
          this.resolveSize(this.quadCount);
        });

        result.on('error', err => {
          this.rejectSize(err);
        });

        return result.map(quad => {
          this.quadCount++;
          return quad;
        });
      }      
      return result;
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
