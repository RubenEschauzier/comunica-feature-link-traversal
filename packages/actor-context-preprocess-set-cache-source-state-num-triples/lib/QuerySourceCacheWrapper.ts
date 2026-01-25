import { KeysQuerySourceIdentifyHypermediaNoneLazy } from '@comunica/context-entries-link-traversal';
import { ActionContext } from '@comunica/core';
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
import { AsyncIterator } from 'asynciterator';
import { RdfStore } from 'rdf-stores';
import { DataFactory } from 'rdf-data-factory';


export class QuerySourceCacheWrapper implements IQuerySource {
  private source: IQuerySource;
  private store: RdfStore | undefined;

  public referenceValue: QuerySourceReference;

  protected DF: DataFactory = new DataFactory();
  protected AF: AlgebraFactory = new AlgebraFactory(this.DF);
  
  public constructor(
    source: IQuerySource
  ) {
    this.source = source;
    this.referenceValue = source.referenceValue;
  }

  public async getSelectorShape(context: IActionContext): Promise<FragmentSelectorShape> {
    return this.source.getSelectorShape(context);
  }

  public async getFilterFactor(context: IActionContext): Promise<number> {
    return this.source.getFilterFactor(context);
  }

  public getSize(){
    return this.store ? this.store.size : 1;
  }

  public async ingestQuads(){
    const store = RdfStore.createDefault();
    const quads = this.source.queryQuads(
      this.AF.createPattern(
          this.DF.variable('s'),
          this.DF.variable('p'),
          this.DF.variable('o'),
          this.DF.variable('g')
      ),
      new ActionContext({ 
        [KeysQuerySourceIdentifyHypermediaNoneLazy.nonConsumingQueryQuads.name]: true
      })
    );
    const promiseConsumedSource = new Promise<RdfStore>((resolve, reject) => {
      quads.on('data', (quad) => store.addQuad(quad));
      quads.on('end', () => {
        this.store = store;
        resolve(store);
      });
      quads.on('error', () => reject("Error importing quads for cached source"));
    })
    return promiseConsumedSource;
  }

  public awaitIngestion(){
    return this.store;
  }
  public queryBindings(
    _operation: Algebra.Operation,
    _context: IActionContext,
    _options?: IQueryBindingsOptions,
  ): BindingsStream {
    throw new Error('queryBindings is not implemented in QuerySourceCacheWrapper');
  }

  public queryQuads(
    operation: Algebra.Operation,
    _context: IActionContext,
  ): AsyncIterator<RDF.Quad> {
    if (!this.store){
      throw new Error("Tried to query quads on cache source before ingesting quads")
    }
    if (isKnownOperation(operation, Algebra.Types.PATTERN) &&
        operation.subject.termType === 'Variable' &&
        operation.predicate.termType === 'Variable' &&
        operation.object.termType === 'Variable' &&
        operation.graph.termType === 'Variable') {
      return this.store.match();
    }
    throw new Error('queryQuads is not implemented in QuerySourceCacheWrapper');
  }

  public countQuads(operation: Algebra.Operation, _context: IActionContext){
    if (!this.store){
      throw new Error("Tried to query quads on cache source before ingesting quads")
    }
    if (isKnownOperation(operation, Algebra.Types.PATTERN)){
      return this.store.countQuads(
        operation.subject,
        operation.predicate,
        operation.object,
        operation.graph
      );
    }
    throw new Error('countQuads only supports pattern operations in QuerySourceCacheWrapper');

  }
  public queryBoolean(
    _operation: Algebra.Ask,
    _context: IActionContext,
  ): Promise<boolean> {
    throw new Error('queryBoolean is not implemented in QuerySourceCacheWrapper');
  }

  public queryVoid(
    _operation: Algebra.Operation,
    _context: IActionContext,
  ): Promise<void> {
    throw new Error('queryVoid is not implemented in QuerySourceCacheWrapper');
  }

  public toString(): string {
    return `QuerySourceCacheWrapper(${this.source.toString})`;
  }
}
