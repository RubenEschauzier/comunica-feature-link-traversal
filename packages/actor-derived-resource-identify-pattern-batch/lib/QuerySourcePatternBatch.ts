import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { IBatchQuerySource } from '@comunica/bus-derived-resource-identify';
import type {
  BindingsStream,
  ComunicaDataFactory,
  FragmentSelectorShape,
  IActionContext,
  QuerySourceReference,
} from '@comunica/types';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { wrap } from 'asynciterator';
import { termToString } from 'rdf-string';

const POSITIONS = [ [ 's', 'subject' ], [ 'p', 'predicate' ], [ 'o', 'object' ], [ 'g', 'graph' ] ] as const;

/**
 * The query source of a pattern batch derived resource: one that answers any number of independent
 * quad patterns, up to a limit, in a single request.
 *
 * The patterns go in the query string as `s0`, `p0`, `o0`, `g0`, `s1`, ..., each term in the syntax
 * of `rdf-string`, with `?x` for a variable. A pattern in the default graph leaves out its graph, as
 * the server then matches every graph, which is what the single pattern templates do as well.
 */
export class QuerySourcePatternBatch implements IBatchQuerySource {
  public readonly referenceValue: QuerySourceReference;
  protected readonly selectorShape: FragmentSelectorShape;

  public constructor(
    protected readonly url: string,
    public readonly maxBatchSize: number,
    protected readonly mediatorDereferenceRdf: MediatorDereferenceRdf,
    dataFactory: ComunicaDataFactory,
  ) {
    this.referenceValue = url;
    const algebraFactory = new AlgebraFactory(dataFactory);
    const variables = [ 's', 'p', 'o', 'g' ].map(name => dataFactory.variable(name));
    this.selectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern: algebraFactory.createPattern(variables[0], variables[1], variables[2], variables[3]),
      },
      variablesOptional: variables,
    };
  }

  public async getSelectorShape(_context?: IActionContext): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public async getFilterFactor(_context: IActionContext): Promise<number> {
    return 0;
  }

  public queryQuads(operation: Algebra.Operation, context: IActionContext): AsyncIterator<RDF.Quad> {
    if (!isKnownOperation(operation, Algebra.Types.PATTERN)) {
      throw new Error(`${this.constructor.name} only accepts patterns, got: ${operation.type}`);
    }
    return this.queryQuadsBatch([ operation ], context);
  }

  public queryQuadsBatch(patterns: Algebra.Pattern[], context: IActionContext): AsyncIterator<RDF.Quad> {
    if (patterns.length === 0 || patterns.length > this.maxBatchSize) {
      throw new Error(`${this.constructor.name} takes 1 to ${this.maxBatchSize} patterns, got ${patterns.length}`);
    }
    const url = this.getBatchUrl(patterns);
    return wrap<RDF.Quad>(this.mediatorDereferenceRdf.mediate({ context, url })
      .then(output => <AsyncIterator<RDF.Quad>> wrap(output.data)));
  }

  /**
   * The url asking for all the given patterns.
   */
  public getBatchUrl(patterns: Algebra.Pattern[]): string {
    const parameters = new URLSearchParams();
    patterns.forEach((pattern, index) => {
      for (const [ key, position ] of POSITIONS) {
        const term = pattern[position];
        if (position === 'graph' && term.termType === 'DefaultGraph') {
          continue;
        }
        parameters.set(`${key}${index}`, term.termType === 'Variable' ? `?${term.value}` : termToString(term));
      }
    });
    return `${this.url}?${parameters.toString()}`;
  }

  public queryBindings(_operation: Algebra.Operation, _context: IActionContext): BindingsStream {
    throw new Error(`queryBindings is not implemented in ${this.constructor.name}`);
  }

  public queryBoolean(_operation: Algebra.Ask, _context: IActionContext): Promise<boolean> {
    throw new Error(`queryBoolean is not implemented in ${this.constructor.name}`);
  }

  public queryVoid(_operation: Algebra.Operation, _context: IActionContext): Promise<void> {
    throw new Error(`queryVoid is not implemented in ${this.constructor.name}`);
  }

  public toString(): string {
    return `${this.constructor.name}(${this.url})`;
  }
}
