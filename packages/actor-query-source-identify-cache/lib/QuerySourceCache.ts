import { ICacheKey, IViewKey } from '@comunica/cache-manager-entries';
import type {
  BindingsStream,
  ComunicaDataFactory,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
  ISourceState,
} from '@comunica/types';
import { ICacheView } from '@comunica/types-link-traversal';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { ClosableTransformIterator } from '@comunica/utils-iterator';
import type * as RDF from '@rdfjs/types';
import { type AsyncIterator, wrap as wrapAsyncIterator} from 'asynciterator';
import { UnionIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { PersistentCacheManager } from '@comunica/actor-context-preprocess-set-persistent-cache-manager';

/**
 * A query source that operates sources obtained from a link queue.
 */
export class QuerySourceCache implements IQuerySource {
  public readonly referenceValue: string;
  protected readonly selectorShape: FragmentSelectorShape;

  private readonly dataFactory: ComunicaDataFactory = new DataFactory();
  private readonly AF = new AlgebraFactory(this.dataFactory);
  
  private readonly persistentCacheManager: PersistentCacheManager;
  private readonly getSourceView: IViewKey<
    ISourceState,
    { url: string, mode: 'get' | 'query', operation: Algebra.Operation }, 
    BindingsStream | ISourceState
  >;
  private readonly cacheKey: ICacheKey<ISourceState, ISourceState, { headers: Headers }>

  public constructor(
    persistentCacheManager: PersistentCacheManager,
    cacheKey: ICacheKey<ISourceState, ISourceState, { headers: Headers }>,
    getSourceView: IViewKey<
        ISourceState,
        { url: string, mode: 'get' | 'query', operation: Algebra.Operation }, 
        BindingsStream | ISourceState
    >, 
  ) {
    this.referenceValue = 'cacheSource';
    this.selectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern: this.AF.createPattern(
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
    this.persistentCacheManager = persistentCacheManager;
    this.getSourceView = getSourceView;
    this.cacheKey = cacheKey
  }

  public async getSelectorShape(_context: IActionContext): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public async getFilterFactor(): Promise<number> {
    return 0;
  }

  public queryBindings(
    operation: Algebra.Operation,
    _context: IActionContext,
    _options?: IQueryBindingsOptions,
  ): BindingsStream {
    const streamPromise = this.persistentCacheManager.getFromCache(
        this.cacheKey,
        this.getSourceView,
        { url: "", operation, mode: 'query'}
    );
    if (streamPromise === undefined){
      throw new Error("Tried to query a cache that does not exist");
    }
    return wrapAsyncIterator(<Promise<BindingsStream>> streamPromise, { autoStart: false});
  }

  public queryQuads(
    operation: Algebra.Operation,
    _context: IActionContext,
  ): AsyncIterator<RDF.Quad> {
    if (isKnownOperation(operation, Algebra.Types.PATTERN)) {
      return wrapAsyncIterator<RDF.Quad>(
        this.source.match(operation.subject, operation.predicate, operation.object, operation.graph),
        { autoStart: false },
      );
    }
    throw new Error('queryQuads is not implemented in QuerySourceRdfJs');
  }

  public queryBoolean(): Promise<boolean> {
    throw new Error('queryBoolean is not implemented in QuerySourceLinkTraversal');
  }

  public queryVoid(): Promise<void> {
    throw new Error('queryVoid is not implemented in QuerySourceLinkTraversal');
  }

  public toString(): string {
    return `QuerySourceLinkTraversal(${this.referenceValue})`;
  }
}
