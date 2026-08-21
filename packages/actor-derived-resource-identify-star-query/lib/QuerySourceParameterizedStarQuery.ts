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
import { Variable } from 'rdf-data-factory';

export class QuerySourceParameterizedStarQuery implements IQuerySource {
  public readonly referenceValue: QuerySourceReference;
  
  protected readonly dataFactory: ComunicaDataFactory;
  protected readonly algebraFactory: AlgebraFactory;

  protected readonly parameterizedPatterns: IParameterizedPattern[];

  protected readonly mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;

  protected readonly template: string;
  protected readonly parameters: Set<string>;
  protected readonly selectorShape: FragmentSelectorShape;

  public constructor(
    template: string,
    operation: Algebra.Construct,
    parameters: Set<string>,
    mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink,
    dataFactory: ComunicaDataFactory,
  ) {
    if (!isKnownOperation(operation.input, Algebra.Types.BGP)){
        throw new Error(`Non-BGP passed to star query source`);
    }
    this.referenceValue = template;
    this.template = template;
    this.mediatorQuerySourceDereferenceLink = mediatorQuerySourceDereferenceLink;
    this.parameters = parameters;
    this.dataFactory = dataFactory;

    this.algebraFactory = new AlgebraFactory(this.dataFactory);


    const cleanTerm = (term: RDF.Term): RDF.Term => {
      if (term.termType === 'Variable') {
        return this.dataFactory.variable(term.value.replace(/^__param_/, ''));
      }
      return term;
    };

    this.parameterizedPatterns = this.buildParameterMapping(
      operation.input.patterns, parameters
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
          .flatMap((pattern) =>
            [...Object.values(pattern)]
              .filter((parameterName: string | undefined) => parameterName !== undefined)
              .map((parameterName: string) => this.dataFactory.variable(parameterName)),
          )
          .map((variable) => [variable.value, variable]),
      ).values(),
    );    


    this.selectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern: cleanBgp,
      },
      variablesOptional
    };
  }

  private buildParameterMapping(
      patterns: Algebra.Pattern[], 
      parameterNames: Set<string>
    ): IParameterizedPattern[] {
      const extractParam = (term: RDF.Term): string | undefined => {
        if (term.termType === 'Variable') {
          const cleaned = term.value.replace(/^__param_/, '');
          return parameterNames.has(cleaned) ? cleaned : undefined;
        }
        return undefined;
      };
      
      return patterns.map((pattern) => { 
        return {
          subject: extractParam(pattern.subject),
          predicate: extractParam(pattern.predicate),
          object: extractParam(pattern.object),
          graph: extractParam(pattern.graph)
        }
      });
    }
  
  

  public async getSelectorShape(): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public async getFilterFactor(): Promise<number> {
    return 0;
  }


  public queryQuads(operation: Algebra.Operation, context: IActionContext): AsyncIterator<RDF.Quad> {
    if (!isKnownOperation(operation, Algebra.Types.BGP)) {
      throw new Error(`QuerySourceParameterizedStarQuery only accepts BGPs, got: ${operation.type}`);
    }

    if(!doesShapeAcceptOperation(this.selectorShape, operation)){
      throw new Error(`Attempted queryQuads using operation not supported by QuerySourceStarQuery`)
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

  private async resolveAndExecuteQuads(operation: Algebra.Bgp, context: IActionContext): Promise<AsyncIterator<RDF.Quad>> {
    // Fill in the parameter values of the template
    const replaceParam = (url: string, param: string, value: RDF.Term) => {
      const regex = new RegExp(`(?:\\{|%7B)${param}(?:\\}|%7D)`, 'g');
      const stringValue = toNT(value);
      return url.replace(regex, encodeURIComponent(stringValue));
    };

    let filledTemplateUri = this.template;
    console.log("Split!");
    console.log(filledTemplateUri)
    // TODO: Fill in the url template using the predicates in the operation


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