import type {
  BindingsStream,
  ComunicaDataFactory,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
  QuerySourceReference,
} from '@comunica/types';
import { Algebra, AlgebraFactory } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { EmptyIterator } from 'asynciterator';

export class QuerySourceStub implements IQuerySource {
  protected readonly selectorShape: FragmentSelectorShape;
  public referenceValue: QuerySourceReference;

  public constructor(
    protected readonly dataFactory: ComunicaDataFactory,
    protected readonly url: string,
  ) {
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
    _operation: Algebra.Operation,
    _context: IActionContext,
    _options?: IQueryBindingsOptions,
  ): BindingsStream {
    return new EmptyIterator<RDF.Bindings>;
  }

  public queryQuads(
    _operation: Algebra.Operation,
    _context: IActionContext,
  ): AsyncIterator<RDF.Quad> {
    return new EmptyIterator<RDF.Quad>;
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
