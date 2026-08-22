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
    template: string,
    operation: Algebra.Pattern,
    parameters: Set<string>,
    mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink,
    dataFactory: ComunicaDataFactory,
  ) {
    this.referenceValue = template;
    this.template = template;
    this.mediatorQuerySourceDereferenceLink = mediatorQuerySourceDereferenceLink;
    this.dataFactory = dataFactory;

    this.algebraFactory = new AlgebraFactory(this.dataFactory);
    
    // Interface with all parameters that should be filled in when doing queryBindings
    this.parameterizedPattern  = this.buildParameterMapping(operation, parameters);

    const cleanTerm = (term: RDF.Term): RDF.Term => {
      if (term.termType === 'Variable') {
        return this.dataFactory.variable(term.value.replace(/^__param_/, ''));
      }
      return term;
    };

    const cleanPattern = this.algebraFactory.createPattern(
      cleanTerm(operation.subject),
      cleanTerm(operation.predicate),
      cleanTerm(operation.object),
      cleanTerm(operation.graph),
    );

    this.selectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern: cleanPattern,
      },
      variablesOptional: [
        ...[...Object.values(this.parameterizedPattern)]
          .filter((parameterName: string) => parameterName !== undefined)
          .map((parameterName: string) => this.dataFactory.variable(parameterName)),
      ],
    };
  }

  private buildParameterMapping(
    pattern: Algebra.Pattern, 
    parameterNames: Set<string>
  ): IParameterizedPattern {
    const extractParam = (term: RDF.Term): string | undefined => {
      if (term.termType === 'Variable') {
        const cleaned = term.value.replace(/^__param_/, '');
        return parameterNames.has(cleaned) ? cleaned : undefined;
      }
      return undefined;
    };

    return {
      subject: extractParam(pattern.subject),
      predicate: extractParam(pattern.predicate),
      object: extractParam(pattern.object),
      graph: extractParam(pattern.graph),
    };
  }


  public async getSelectorShape(): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public async getFilterFactor(): Promise<number> {
    return 0;
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
    const replaceParam = (url: string, param: string, value: RDF.Term, variableName: string) => {
      const regex = new RegExp(`(?:\\{|%7b)${param}(?:\\}|%7d)`, 'gi');
      // Normalize the variable names to ensure they're passed to the underlying derived resource correctly.
      // Variable names such as "__comunica:pp_var_subj" cause issues. 
      if (value.termType === 'Variable'){
        value = this.dataFactory.variable(variableName);
      }
      const stringValue = toNT(value);
      return url.replace(regex, encodeURIComponent(stringValue));
    };

    let filledTemplateUri = this.template;
    if (this.parameterizedPattern.subject) {
      filledTemplateUri = replaceParam(filledTemplateUri, this.parameterizedPattern.subject, operation.subject, 's');
    }
    if (this.parameterizedPattern.predicate) {
      filledTemplateUri = replaceParam(filledTemplateUri, this.parameterizedPattern.predicate, operation.predicate, 'p');
    }
    if (this.parameterizedPattern.object) {
      filledTemplateUri = replaceParam(filledTemplateUri, this.parameterizedPattern.object, operation.object, 'o');
    }
    if (this.parameterizedPattern.graph) {
      filledTemplateUri = replaceParam(filledTemplateUri, this.parameterizedPattern.graph, operation.graph, 'g');
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