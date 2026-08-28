import { ActorDerivedResourceIdentify, IActionDerivedResourceIdentify, IActorDerivedResourceIdentifyOutput, IActorDerivedResourceIdentifyArgs } from '@comunica/bus-derived-resource-identify';
import { TestResult, IActorTest, passTestVoidWithSideData, failTest } from '@comunica/core';
import type * as RDF from '@rdfjs/types';
import { Algebra, isKnownOperation } from '@comunica/utils-algebra';
import type { MediatorDereference } from '@comunica/bus-dereference';
import type { MediatorQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { MediatorQueryParse } from '@comunica/bus-query-parse';
import { KeysInitQuery } from '@comunica/context-entries';
import { ComunicaDataFactory } from '@comunica/types';
import { DataFactory } from 'rdf-data-factory';
import { QuerySourceParameterizedStarQuery } from './QuerySourceParameterizedStarQuery';

/**
 * A comunica Star Query Derived Resource Identify Actor.
 */
export class ActorDerivedResourceIdentifyStarQuery extends ActorDerivedResourceIdentify<IStarQuerySideData> {
  protected readonly mediatorDereference: MediatorDereference;
  protected readonly mediatorQuerySourceDereferenceLink?: MediatorQuerySourceDereferenceLink;
  protected readonly mediatorQueryParse: MediatorQueryParse;
  
  protected readonly dataFactory: ComunicaDataFactory = new DataFactory();
  public constructor(args: IActorDerivedResourceIdentifyStarQueryArgs) {
    super(args);
    this.mediatorDereference = args.mediatorDereference;
    this.mediatorQuerySourceDereferenceLink = args.mediatorQuerySourceDereferenceLink;
    this.mediatorQueryParse = args.mediatorQueryParse;
  }

  public async test(action: IActionDerivedResourceIdentify): Promise<TestResult<IActorTest, IStarQuerySideData>> {
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

    if (!isKnownOperation(queryParseOutput.operation, Algebra.Types.PROJECT)){
      return failTest(`${this.name} only works with select templates`);
    } 

    const operationInput = queryParseOutput.operation.input;
    if (!isKnownOperation(operationInput, Algebra.Types.BGP)){
      return failTest(`${this.name} requires a WHERE clause with only a BGP`);
    }

    if (!this.isStarShaped(operationInput)){
      return failTest(`${this.name} requires a star-shaped query with all objects different and variable subject`);
    }

    if (!this.selectsStarVariables(operationInput, queryParseOutput.operation.variables)){
      return failTest(`${this.name} requires the select clause to project all star variables`);
    }
      
    if (!(parameters.size >= operationInput.patterns.length) || 
      !this.allPredicatesParameters(operationInput.patterns, parameters) || 
      operationInput.patterns.length !== new Set(operationInput.patterns.map(p => p.predicate.value)).size
    ){
      return failTest(`${this.name} requires exclusively predicate parameters without repeats`);
    }

    return passTestVoidWithSideData<IStarQuerySideData>({
      parameters,
      operation: queryParseOutput.operation,
    });
  }

  public async run(
    action: IActionDerivedResourceIdentify,
    sideData: IStarQuerySideData,
  ): Promise<IActorDerivedResourceIdentifyOutput> {
    // TEMP FOR COMPILING
    const templateString = new URL(
        action.derivedResourceUnidentified.template,
        action.derivedResourceUnidentified.baseUrl
    ).href;

    const proxySource = new QuerySourceParameterizedStarQuery(
      templateString,
      sideData.operation,
      sideData.parameters,
      this.mediatorDereference,
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
   * Checks if operation adheres to required star shape.
   * Specifically something like this:
   * WHERE {
   *  ?s ?p1 ?o1
   *  ?s ?p2 ?o2
   * }
   * @param operation 
   * @returns 
   */
  public isStarShaped(operation: Algebra.Bgp): boolean {
    if (operation.patterns.length === 0 || operation.patterns.length === 1) {
      return false;
    }

    const rootSubject = operation.patterns[0].subject;
    if (rootSubject.termType !== 'Variable') {
      return false;
    }

    const allSubjectSame = operation.patterns.every(pattern => 
      pattern.subject.equals(rootSubject)
    );

    const seenObjects = new Set<string>();
    const allObjectsValidAndDistinct = !operation.patterns.some(pattern => {
      // Ensure the object is a variable and not equal to the central subject
      if (pattern.object.termType !== 'Variable' || pattern.object.equals(rootSubject)) {
        return true;
      }

      const objectId = pattern.object.value;
      if (seenObjects.has(objectId)) {
        return true;
      }
      
      seenObjects.add(objectId);
      return false;
    });

    return allSubjectSame && allObjectsValidAndDistinct;
  }

  public constructsEqualStar(operation: Algebra.Bgp, templatePatterns: Algebra.Pattern[]) {
    const patterns = operation.patterns;

    if (patterns.length !== templatePatterns.length) {
      return false;
    }

    const hashPattern = (p: Algebra.Pattern) => 
      `${p.subject.termType}:${p.subject.value}|${p.predicate.termType}:${p.predicate.value}|${p.object.termType}:${p.object.value}|${p.graph.termType}:${p.graph.value}`;
    
    const templateCounts = new Map<string, number>();

    for (const pattern of templatePatterns) {
      const hash = hashPattern(pattern);
      templateCounts.set(hash, (templateCounts.get(hash) || 0) + 1);
    }

    for (const pattern of patterns) {
      const hash = hashPattern(pattern);
      const count = templateCounts.get(hash) || 0;

      // Triple is missing in map or frequency is exhausted
      if (count === 0) {
        return false;
      }
      
      templateCounts.set(hash, count - 1);
    }  
    return true;
  }

  public selectsStarVariables(operation: Algebra.Bgp, variables: RDF.Variable[]): boolean {
    const varSet = new Set(variables.map(v => v.value));
    if (!varSet.has(operation.patterns[0].subject.value)) {
      return false;
    }
    return operation.patterns.every(pattern => varSet.has(pattern.object.value));
  }

  public allPredicatesParameters(patterns: Algebra.Pattern[], parameters: Set<string>){
    return patterns.every((pattern) => 
      parameters.has(pattern.predicate.value.replace('__param_', ''))
    )
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


export interface IActorDerivedResourceIdentifyStarQueryArgs 
extends IActorDerivedResourceIdentifyArgs<IStarQuerySideData> {
  mediatorDereference: MediatorDereference;
  mediatorQuerySourceDereferenceLink?: MediatorQuerySourceDereferenceLink;
  mediatorQueryParse: MediatorQueryParse;
}

export interface IStarQuerySideData {
  parameters: Set<string>;
  operation: Algebra.Project;
}