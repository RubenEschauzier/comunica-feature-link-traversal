import { ActorDerivedResourceIdentify, IActionDerivedResourceIdentify, IActorDerivedResourceIdentifyOutput, IActorDerivedResourceIdentifyArgs } from '@comunica/bus-derived-resource-identify';
import { MediatorQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import type { MediatorQueryParse } from '@comunica/bus-query-parse';
import { TestResult, IActorTest, passTestVoid, failTest, ActionContext, passTestVoidWithSideData } from '@comunica/core';
import { QuerySourceParameterizedPattern } from './QuerySourceParameterizedPattern';
import { DataFactory } from 'rdf-data-factory';
import { KeysInitQuery } from '@comunica/context-entries';
import type * as RDF from '@rdfjs/types';
import { Algebra, isKnownOperation } from '@comunica/utils-algebra';
import { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';

/**
 * A comunica Triple Pattern Query Derived Resource Identify Actor.
 */
export class ActorDerivedResourceIdentifyTriplePatternQuery extends ActorDerivedResourceIdentify<ITriplePatternSideData> {
  protected readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  protected readonly mediatorQueryParse: MediatorQueryParse;
  protected readonly dataFactory: DataFactory = new DataFactory();

  public constructor(args: IActorDerivedResourceIdentifyTriplePatternQueryArgs) {
    super(args);
    this.mediatorDereferenceRdf = args.mediatorDereferenceRdf;
    this.mediatorQueryParse = args.mediatorQueryParse;
  }

  public async test(action: IActionDerivedResourceIdentify): Promise<TestResult<IActorTest, ITriplePatternSideData>> {
    // TODO: What to do with quad patterns
    let context = action.context;
    const filter = action.derivedResourceUnidentified.filter;
    const parameters = this.extractTemplateParams(action.derivedResourceUnidentified.template);

    // General triple pattern queries require a template
    if (parameters.size === 0){
      return failTest(`${this.name} requires parameters in template of derived resource`);
    }

    // Remove template values and add variables in their place so we can parse the query
    const {normalized, newVariables} = this.normalizeQuery(filter, parameters);    

    let queryParseOutput;
    try {
      const baseIRI: string | undefined = context.get(KeysInitQuery.baseIRI);
      const queryFormat: RDF.QueryFormat = context.get(KeysInitQuery.queryFormat)!;
      queryParseOutput = await this.mediatorQueryParse.mediate(
        { 
          context, 
          query: normalized,
          queryFormat, 
          baseIRI 
        }
      );
    }
    catch (err: any) {
      return failTest(`${this.name} parsing query failed with: ${err.message}`);
    }

    if (!isKnownOperation(queryParseOutput.operation, Algebra.Types.CONSTRUCT)){
      return failTest(`${this.name} only works with construct templates`);
    } 

    if (queryParseOutput.operation.template.length != 1){
      return failTest(`${this.name} requires a template with one constructed triple`);
    }

    if (!isKnownOperation(queryParseOutput.operation.input, Algebra.Types.BGP) ||
    queryParseOutput.operation.input.patterns.length != 1){
      return failTest(`${this.name} requires a WHERE clause with only one triple pattern`);
    }

    const inputPattern = queryParseOutput.operation.input.patterns[0];
    const patternTerms = [inputPattern.subject, inputPattern.predicate, inputPattern.object];

    const templatePattern = queryParseOutput.operation.template[0];
    if (!templatePattern.equals(inputPattern)) {
      return failTest(`${this.name} requires the CONSTRUCT template and WHERE pattern to be identical`);
    }

    // Ensure all pattern terms are variables
    if (!patternTerms.every(term => term.termType === 'Variable')) {
      return failTest(`${this.name} requires all terms in the pattern to be variables`);
    }

    // Ensure at least one variable corresponds to a template parameter
    const containsParam = patternTerms.some(term => 
      newVariables.has(`?${term.value}`)
    );

    if (!containsParam) {
      return failTest(`${this.name} requires at least one template parameter in the pattern`);
    }

    return passTestVoidWithSideData<ITriplePatternSideData>({
      parameters,
      operation: queryParseOutput.operation,
    });
  }

  public async run(
    action: IActionDerivedResourceIdentify,
    sideData: ITriplePatternSideData,
  ): Promise<IActorDerivedResourceIdentifyOutput> {
    const templateString = new URL(
       action.derivedResourceUnidentified.template,
       action.derivedResourceUnidentified.baseUrl
    ).href;

    const proxySource = new QuerySourceParameterizedPattern(
      templateString,
      // The test function guarantees CONSTRUCT = WHERE clause and template is one pattern
      sideData.operation.template[0],
      sideData.parameters,
      this.mediatorDereferenceRdf,
      this.dataFactory,
    );

    const derivedResource: IActorDerivedResourceIdentifyOutput = {
      derivedResourceIdentified: {
        iri: templateString,
        derivedResourceSelectorShape: await proxySource.getSelectorShape(),
        ...action.derivedResourceUnidentified,
        querySource: proxySource,
        resourceCoefficients:  {
          selectivity: 1,
          requests: 1,
          compute: 5
        }        
      }
    }
    return derivedResource;
  }

  public extractTemplateParams(templateStr: string) {
    const matches = templateStr.matchAll(/\{([^}]+)\}/g);
    return new Set(Array.from(matches, m => m[1]));
  }

  /**
   * Takes the parameters from the template (in { } brackets) and replaces those
   * with temporary parameters
   * @param rawQuery 
   * @param paramNames 
   * @returns 
   */
  public normalizeQuery(rawQuery: string, paramNames: Set<string>) {
    let normalized = rawQuery;
    
    const newVariables = new Set();
    for (const param of paramNames) {
      // Escapes and replaces $param$ with a distinct valid variable
      const regex = new RegExp(`\\$${param}\\$`, 'g');
      normalized = normalized.replace(regex, `?__param_${param}`);
      newVariables.add(`?__param_${param}`);
    }
    return {
      normalized,
      newVariables,
    };
  }
}

export interface IActorDerivedResourceIdentifyTriplePatternQueryArgs 
extends IActorDerivedResourceIdentifyArgs<ITriplePatternSideData> {
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  mediatorQueryParse: MediatorQueryParse;
}

export interface ITriplePatternSideData {
  parameters: Set<string>;
  operation: Algebra.Construct;
}