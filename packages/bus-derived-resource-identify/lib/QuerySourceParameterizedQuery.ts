import type { IActorDereferenceOutput, MediatorDereference } from '@comunica/bus-dereference';
import type {
  IQuerySource,
  IActionContext,
  FragmentSelectorShape,
  ComunicaDataFactory,
  QuerySourceReference,
  BindingsStream,
  IQueryBindingsOptions,
} from '@comunica/types';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { canAnswerBgp } from '@comunica/utils-query-operation';
import toNT from '@rdfjs/to-ntriples';
import * as RDF from '@rdfjs/types';
import { AsyncIterator, TransformIterator, wrap } from 'asynciterator';
import { SparqlJsonParser } from 'sparqljson-parse';

/**
 * A query source that answers a BGP by filling the parameters of a URI template
 * of a derived resource, and dereferencing the resulting URI as SPARQL JSON results.
 *
 * This source is agnostic of the shape of the query in the derived resource, the actor
 * that identified the derived resource is responsible for validating that shape.
 */
export class QuerySourceParameterizedQuery implements IQuerySource {
  public readonly referenceValue: QuerySourceReference;

  protected readonly dataFactory: ComunicaDataFactory;
  protected readonly algebraFactory: AlgebraFactory;
  protected readonly bindingsFactory: BindingsFactory;

  protected readonly parameterizedPatterns: IParameterizedPattern[];
  protected readonly mediatorDereference: MediatorDereference;

  protected readonly template: string;
  protected readonly parameters: Set<string>;
  protected readonly selectorShape: FragmentSelectorShape;
  protected readonly orderPatterns: (patterns: Algebra.Pattern[]) => Algebra.Pattern[];

  /**
   * @param template The URI template of the derived resource, with parameters between curly braces.
   * @param operation The parsed query of the derived resource, with parameters as `__param_` variables.
   * @param parameters The names of the parameters occurring in the template.
   * @param mediatorDereference Mediator used to dereference filled-in templates.
   * @param dataFactory The data factory to create terms with.
   * @param sourceLabel Human readable name of the query shape, used in error messages.
   * @param orderPatterns Brings the patterns of a BGP into the order in which their parameters
   *                      occur in the template. Applied to the patterns of the derived resource
   *                      and to the patterns of every incoming query, so that both align.
   *                      Only needed for shapes in which the order of the patterns is meaningful.
   */
  public constructor(
    template: string,
    operation: Algebra.Project,
    parameters: Set<string>,
    mediatorDereference: MediatorDereference,
    dataFactory: ComunicaDataFactory,
    sourceLabel = 'parameterized query source',
    orderPatterns: (patterns: Algebra.Pattern[]) => Algebra.Pattern[] = patterns => patterns,
  ) {
    if (!isKnownOperation(operation.input, Algebra.Types.BGP)) {
      throw new Error(`Non-BGP passed to ${sourceLabel}`);
    }
    this.orderPatterns = orderPatterns;
    this.referenceValue = template;
    this.template = template;
    this.mediatorDereference = mediatorDereference;
    this.parameters = parameters;
    this.dataFactory = dataFactory;
    this.algebraFactory = new AlgebraFactory(this.dataFactory);
    this.bindingsFactory = new BindingsFactory(this.dataFactory);

    const cleanTerm = (term: RDF.Term): RDF.Term => {
      if (term.termType === 'Variable') {
        return this.dataFactory.variable(term.value.replace(/^__param_/, ''));
      }
      return term;
    };

    const orderedPatterns = orderPatterns(operation.input.patterns);

    this.parameterizedPatterns = this.buildParameterMapping(
      orderedPatterns,
      parameters,
    );

    const cleanPatterns = orderedPatterns.map(pattern =>
      this.algebraFactory.createPattern(
        cleanTerm(pattern.subject),
        cleanTerm(pattern.predicate),
        cleanTerm(pattern.object),
        cleanTerm(pattern.graph),
      ),
    );
    const cleanBgp = this.algebraFactory.createBgp(cleanPatterns);

    const variablesOptional = Array.from(
      new Map(
        this.parameterizedPatterns
          .flatMap(pattern =>
            [...Object.values(pattern)]
              .filter((param): param is string => param !== undefined)
              .map(param => this.dataFactory.variable(param)),
          )
          .map(variable => [variable.value, variable]),
      ).values(),
    );

    this.selectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern: cleanBgp,
      },
      variablesOptional,
    };
  }

  private buildParameterMapping(
    patterns: Algebra.Pattern[],
    parameterNames: Set<string>,
  ): IParameterizedPattern[] {
    const extractParam = (term: RDF.Term): string | undefined => {
      if (term.termType === 'Variable') {
        const cleaned = term.value.replace(/^__param_/, '');
        return parameterNames.has(cleaned) ? cleaned : undefined;
      }
      return undefined;
    };

    return patterns.map(pattern => ({
      subject: extractParam(pattern.subject),
      predicate: extractParam(pattern.predicate),
      object: extractParam(pattern.object),
      graph: extractParam(pattern.graph),
    }));
  }

  private fillTemplateWithPattern(
    operation: Algebra.Pattern,
    templateUri: string,
    parameterizedPattern: IParameterizedPattern,
    replaceParam: (url: string, param: string, value: RDF.Term, positionPrefix: string) => string,
  ): string {
    if (parameterizedPattern.subject) {
      templateUri = replaceParam(templateUri, parameterizedPattern.subject, operation.subject, 's');
    }
    if (parameterizedPattern.predicate) {
      templateUri = replaceParam(templateUri, parameterizedPattern.predicate, operation.predicate, 'p');
    }
    if (parameterizedPattern.object) {
      templateUri = replaceParam(templateUri, parameterizedPattern.object, operation.object, 'o');
    }
    if (parameterizedPattern.graph) {
      templateUri = replaceParam(templateUri, parameterizedPattern.graph, operation.graph, 'g');
    }
    return templateUri;
  }

  public async getSelectorShape(): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public async getFilterFactor(): Promise<number> {
    return 0;
  }

  public queryQuads(_operation: Algebra.Operation, _context: IActionContext): AsyncIterator<RDF.Quad> {
    throw new Error(`queryQuads is not implemented in ${this.constructor.name}`);
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    if (!isKnownOperation(operation, Algebra.Types.BGP)) {
      throw new Error(`${this.constructor.name} only accepts BGPs, got: ${operation.type}`);
    }

    if (
      this.selectorShape.type !== 'operation' ||
      !canAnswerBgp(
        this.selectorShape,
        operation,
        this.selectorShape.variablesOptional ?? [],
        this.selectorShape.variablesRequired ?? [],
      )
    ) {
      throw new Error(`Attempted queryBindings using operation not supported by ${this.constructor.name}`);
    }

    return new TransformIterator(async () => {
      return await this.resolveAndExecuteBindings(operation, context, options);
    });
  }

  private async resolveAndExecuteBindings(
    operation: Algebra.Bgp,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): Promise<BindingsStream> {
    // Keeps track of variable names mapped so far to avoid unintended collisions across disjoint positions
    const variableMapping: Record<string, string> = {};
    let varCounter = 1;

    const replaceParam = (
      url: string,
      param: string,
      value: RDF.Term,
      positionPrefix: string,
    ): string => {
      const regex = new RegExp(`(?:\\{|%7b)${param}(?:\\}|%7d)`, 'gi');

      let targetTerm = value;
      if (value.termType === 'Variable') {
        const originalVarName = value.value;

        // Reuse mapping if seen before, otherwise allocate a unique name
        if (!variableMapping[originalVarName]) {
          const isInternalVar = originalVarName.startsWith('__comunica') || originalVarName.startsWith('__param');
          variableMapping[originalVarName] = isInternalVar
            ? `${positionPrefix}${varCounter++}`
            : originalVarName;
        }

        targetTerm = this.dataFactory.variable(variableMapping[originalVarName]);
      }

      const stringValue = toNT(targetTerm);
      return url.replace(regex, encodeURIComponent(stringValue));
    };

    // Align the incoming patterns with the parameters of the template, as the engine is free
    // to pass the patterns of the BGP in any order
    const patterns = this.orderPatterns(operation.patterns);

    let filledTemplateUri = this.template;
    for (let i = 0; i < patterns.length; i++) {
      filledTemplateUri = this.fillTemplateWithPattern(
        patterns[i],
        filledTemplateUri,
        this.parameterizedPatterns[i],
        replaceParam,
      );
    }

    const dereferenceResult: IActorDereferenceOutput =
      await this.mediatorDereference.mediate({
        url: filledTemplateUri,
        method: 'GET',
        headers: new Headers({
          Accept: 'application/sparql-results+json',
        }),
        context,
      });

    const parser = new SparqlJsonParser({ dataFactory: this.dataFactory });
    const rawBindingsStream = parser.parseJsonResultsStream(dereferenceResult.data);
    const bindingsStream: BindingsStream = <any> wrap(rawBindingsStream)
      .map(record => this.bindingsFactory.fromRecord(<any> record));

    return bindingsStream;
  }

  public queryBoolean(_operation: Algebra.Ask, _context: IActionContext): Promise<boolean> {
    throw new Error(`queryBoolean is not implemented in ${this.constructor.name}`);
  }

  public queryVoid(_operation: Algebra.Operation, _context: IActionContext): Promise<void> {
    throw new Error(`queryVoid is not implemented in ${this.constructor.name}`);
  }

  public toString(): string {
    return `${this.constructor.name}(${this.template})`;
  }
}

export interface IParameterizedPattern {
  subject?: string;
  predicate?: string;
  object?: string;
  graph?: string;
}
