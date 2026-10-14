import type { IActionContext, IQuerySource } from '@comunica/types';
import type { Algebra } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';

/**
 * A query source that answers several independent patterns in one request.
 */
export interface IBatchQuerySource extends IQuerySource {
  /**
   * The most patterns one request may hold.
   */
  maxBatchSize: number;
  /**
   * The quads matching each of the patterns, all in one stream. A quad matching several of the
   * patterns may occur once for each of them.
   * @param patterns The patterns to match.
   * @param context The action context.
   */
  queryQuadsBatch: (patterns: Algebra.Pattern[], context: IActionContext) => AsyncIterator<RDF.Quad>;
}

/**
 * Whether a query source can answer several patterns in one request.
 */
export function isBatchQuerySource(source: IQuerySource): source is IBatchQuerySource {
  return typeof (<Partial<IBatchQuerySource>> source).queryQuadsBatch === 'function' &&
    typeof (<Partial<IBatchQuerySource>> source).maxBatchSize === 'number';
}
