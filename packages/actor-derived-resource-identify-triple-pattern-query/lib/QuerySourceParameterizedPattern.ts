import { IActorDereferenceOutput } from '@comunica/bus-dereference';
import type { IActorQuerySourceDereferenceLinkOutput, MediatorQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import type {
  IQuerySource,
  IActionContext,
  FragmentSelectorShape,
  ComunicaDataFactory,
  QuerySourceReference,
} from '@comunica/types';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { doesShapeAcceptOperation } from '@comunica/utils-query-operation';
import toNT from '@rdfjs/to-ntriples';
import * as RDF from '@rdfjs/types';
import { AsyncIterator, TransformIterator } from 'asynciterator';

export class QuerySourceParameterizedPattern implements IQuerySource {
  public readonly referenceValue: QuerySourceReference;
  
  protected readonly dataFactory: ComunicaDataFactory;
  protected readonly algebraFactory: AlgebraFactory;

  protected readonly mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;
  
  protected readonly template: string;
  protected readonly parameterizedPattern: IParameterizedPattern;
  protected readonly selectorShape: FragmentSelectorShape;

  public constructor(
    filter: string,
    template: string,
    mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink,
    dataFactory: ComunicaDataFactory,
  ) {
    this.referenceValue = template;
    this.template = template;
    this.mediatorQuerySourceDereferenceLink = mediatorQuerySourceDereferenceLink;
    this.dataFactory = dataFactory;

    this.algebraFactory = new AlgebraFactory(this.dataFactory);
    
    // Captures 3 or 4 terms. The 4th term (graph) is optional.
    const match = filter.match(/WHERE\s*\{\s*([^\s]+)\s+([^\s]+)\s+([^\s]+)(?:\s+([^\s]+))?\s*\.\s*\}/);

    if (!match) {
      throw new Error('Filter must contain exactly one triple or quad pattern in the WHERE clause.');
    }

    // Interface with all parameters that should be filled in when doing queryBindings
    this.parameterizedPattern  = this.buildParameterMapping(match);

    // The pattern with parameters translated to variables
    // TODO this can cause conflicts if parameter name is equal to one of the 'spog' names
    const [, sTerm, pTerm, oTerm, gTerm] = match;
    const pattern = this.algebraFactory.createPattern(
      this.dataFactory.variable(this.parameterizedPattern.subject ?? 's'),
      this.dataFactory.variable(this.parameterizedPattern.predicate ?? 'p'),
      this.dataFactory.variable(this.parameterizedPattern.object ?? 'o'),
      gTerm ? this.dataFactory.variable(this.parameterizedPattern.graph ?? 'g') : this.dataFactory.defaultGraph(),
    )

    this.selectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern,
      },
      variablesRequired: [
        ...[...Object.values(this.parameterizedPattern)]
          .map((parameterName: string) => this.dataFactory.variable(parameterName)),
      ],
    };
  }

  private buildParameterMapping(match: RegExpMatchArray){
    const isParam = (term: string | undefined) => !!term && term.startsWith('$') && term.endsWith('$');
    const parameterName = (term: string): string => term.slice(1, -1);

    const [, sTerm, pTerm, oTerm, gTerm] = match;
    const parametersMapping: IParameterizedPattern = {};
    if (isParam(sTerm)){
      parametersMapping.subject = parameterName(sTerm)
    }
    if (isParam(pTerm)){
      parametersMapping.predicate = parameterName(pTerm)
    }
    if (isParam(oTerm)){
      parametersMapping.object = parameterName(oTerm)
    }
    if (isParam(gTerm)){
      parametersMapping.graph = parameterName(gTerm)
    }
    return parametersMapping;
  }

  public async getSelectorShape(): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public async getFilterFactor(): Promise<number> {
    return 0;
  }

  /**
   * Expands RFC 6570 Level 1 and Level 3 (query) URI templates.
   */
  private expandUrlTemplate(template: string, variables: Record<string, string>): string {
    return template.replace(/\{([?&]?)([^}]+)\}/gu, (_, prefix: string, varNames: string) => {
      const names = varNames.split(',');
      const parts: string[] = [];
      
      for (const name of names) {
        if (variables[name] !== undefined) {
          const encodedValue = encodeURIComponent(variables[name]);
          parts.push(prefix === '?' || prefix === '&' ? `${name}=${encodedValue}` : encodedValue);
        }
      }
      
      if (parts.length === 0) {
        return '';
      }
      if (prefix === '?') {
        return `?${parts.join('&')}`;
      }
      if (prefix === '&') {
        return `&${parts.join('&')}`;
      }
      return parts.join(',');
    });
  }

  public queryQuads(operation: Algebra.Operation, context: IActionContext): AsyncIterator<RDF.Quad> {
    if (!isKnownOperation(operation, Algebra.Types.PATTERN)) {
      throw new Error(`Attempted to pass non-pattern operation '${operation.type}' to QuerySourceParameterizedPattern`);
    }

    if(!doesShapeAcceptOperation(this.selectorShape, operation)){
      throw new Error(`Attempted queryQuads using operation not supported by QuerySourceParameterizedPattern`)
    }

    const quadStreamProxy = new TransformIterator<RDF.Quad, RDF.Quad>();
    
    this.resolveAndExecuteQuads(operation, context)
      .then(stream => { 
        quadStreamProxy.source = stream; 
      })
      .catch(error => {
        quadStreamProxy.destroy(error);
      });

    return quadStreamProxy;
  }

  private async resolveAndExecuteQuads(operation: Algebra.Pattern, context: IActionContext): Promise<AsyncIterator<RDF.Quad>> {
    // Fill in the parameter values of the template
    const replaceParam = (url: string, param: string, value: RDF.Term) => {
      const regex = new RegExp(`(?:\\{|%7B)${param}(?:\\}|%7D)`, 'g');
      const stringValue = toNT(value);
      return url.replace(regex, encodeURIComponent(stringValue));
    };

    let filledTemplateUri = this.template;

    if (this.parameterizedPattern.subject) {
      filledTemplateUri = replaceParam(filledTemplateUri, this.parameterizedPattern.subject, operation.subject);
    }
    if (this.parameterizedPattern.predicate) {
      filledTemplateUri = replaceParam(filledTemplateUri, this.parameterizedPattern.predicate, operation.predicate);
    }
    if (this.parameterizedPattern.object) {
      filledTemplateUri = replaceParam(filledTemplateUri, this.parameterizedPattern.object, operation.object);
    }
    if (this.parameterizedPattern.graph) {
      filledTemplateUri = replaceParam(filledTemplateUri, this.parameterizedPattern.graph, operation.graph);
    }
    
    const dereferenceResult: IActorQuerySourceDereferenceLinkOutput = 
    await this.mediatorQuerySourceDereferenceLink.mediate({
      link: { url: filledTemplateUri },
      context
    });

    // Use variable spog operation as this is only supported operation on QuerySourceFileLazy
    // This still returns the correct results for the operation as the actual operation is executed
    // server-side
    return dereferenceResult.source.queryQuads(
      this.algebraFactory.createPattern(
        this.dataFactory.variable('s'),
        this.dataFactory.variable('p'),
        this.dataFactory.variable('o'),
        this.dataFactory.variable('g'),
      ), context
    );
  }

  public queryBindings(_operation: Algebra.Operation, _context: IActionContext): any {
    throw new Error('queryBindings is not implemented in QuerySourceParameterizedPattern');
  }

  public queryBoolean(_operation: Algebra.Ask, _context: IActionContext): Promise<boolean> {
    throw new Error('queryBoolean is not implemented in QuerySourceParameterizedPattern');
  }

  public queryVoid(_operation: Algebra.Operation, _context: IActionContext): Promise<void> {
    throw new Error('queryVoid is not implemented in QuerySourceParameterizedPattern');
  }

  public toString(): string {
    return `QuerySourceParameterizedPattern(${this.template})`;
  }
}



interface IParameterizedPattern {
  subject?: string;
  predicate?: string;
  object?: string;
  graph?: string;
}