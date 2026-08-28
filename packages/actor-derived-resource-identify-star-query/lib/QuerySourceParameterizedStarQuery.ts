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
import { canAnswerBgp, doesShapeAcceptOperation } from '@comunica/utils-query-operation';
import toNT from '@rdfjs/to-ntriples';
import * as RDF from '@rdfjs/types';
import { AsyncIterator, TransformIterator, wrap } from 'asynciterator';
import { SparqlJsonParser } from 'sparqljson-parse';

export class QuerySourceParameterizedStarQuery implements IQuerySource {
  public readonly referenceValue: QuerySourceReference;
  
  protected readonly dataFactory: ComunicaDataFactory;
  protected readonly algebraFactory: AlgebraFactory;
  protected readonly bindingsFactory: BindingsFactory;

  protected readonly parameterizedPatterns: IParameterizedPattern[];
  protected readonly mediatorDereference: MediatorDereference;

  protected readonly template: string;
  protected readonly parameters: Set<string>;
  protected readonly selectorShape: FragmentSelectorShape;

  public constructor(
    template: string,
    operation: Algebra.Project,
    parameters: Set<string>,
    mediatorDereference: MediatorDereference,
    dataFactory: ComunicaDataFactory,
  ) {
    if (!isKnownOperation(operation.input, Algebra.Types.BGP)) {
      throw new Error(`Non-BGP passed to star query source`);
    }
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

    this.parameterizedPatterns = this.buildParameterMapping(
      operation.input.patterns,
      parameters,
    );

    const cleanPatterns = operation.input.patterns.map(pattern =>
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
    throw new Error('queryQuads is not implemented in QuerySourceParameterizedStarQuery');
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    if (!isKnownOperation(operation, Algebra.Types.BGP)) {
      throw new Error(`QuerySourceParameterizedStarQuery only accepts BGPs, got: ${operation.type}`);
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
      throw new Error(`Attempted queryBindings using operation not supported by QuerySourceStarQuery`);
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

    let filledTemplateUri = this.template;
    for (let i = 0; i < operation.patterns.length; i++) {
      filledTemplateUri = this.fillTemplateWithPattern(
        operation.patterns[i],
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
    throw new Error('queryBoolean is not implemented in QuerySourceParameterizedStarQuery');
  }

  public queryVoid(_operation: Algebra.Operation, _context: IActionContext): Promise<void> {
    throw new Error('queryVoid is not implemented in QuerySourceParameterizedStarQuery');
  }

  public toString(): string {
    return `QuerySourceParameterizedStarQuery(${this.template})`;
  }
}

interface IParameterizedPattern {
  subject?: string;
  predicate?: string;
  object?: string;
  graph?: string;
}