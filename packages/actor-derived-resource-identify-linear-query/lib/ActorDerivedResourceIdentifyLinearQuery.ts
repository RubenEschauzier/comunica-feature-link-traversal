import {ActorDerivedResourceIdentify, IActionDerivedResourceIdentify, IActorDerivedResourceIdentifyOutput, IActorDerivedResourceIdentifyArgs, extractTemplateParams, normalizeQuery, FilterParseCache } from '@comunica/bus-derived-resource-identify';
import { TestResult, IActorTest, passTestVoidWithSideData, failTest } from '@comunica/core';
import type * as RDF from '@rdfjs/types';
import { Algebra, isKnownOperation } from '@comunica/utils-algebra';
import type { MediatorDereference } from '@comunica/bus-dereference';
import { MediatorQueryParse } from '@comunica/bus-query-parse';
import { KeysInitQuery } from '@comunica/context-entries';
import { ComunicaDataFactory } from '@comunica/types';
import { DataFactory } from 'rdf-data-factory';
import { findLinearOrder } from './LinearShape';
import { QuerySourceParameterizedLinearQuery } from './QuerySourceParameterizedLinearQuery';

/**
 * A comunica Linear Query Derived Resource Identify Actor.
 */
export class ActorDerivedResourceIdentifyLinearQuery extends ActorDerivedResourceIdentify<ILinearQuerySideData> {
  /**
   * Filters are identical on every pod, so parsing them once covers every pod after the first.
   */
  protected readonly filterParseCache = new FilterParseCache();

  protected readonly mediatorDereference: MediatorDereference;
  protected readonly mediatorQueryParse: MediatorQueryParse;

  protected readonly dataFactory: ComunicaDataFactory = new DataFactory();

  public constructor(args: IActorDerivedResourceIdentifyLinearQueryArgs) {
    super(args);
    this.mediatorDereference = args.mediatorDereference;
    this.mediatorQueryParse = args.mediatorQueryParse;
  }

  public async test(action: IActionDerivedResourceIdentify): Promise<TestResult<IActorTest, ILinearQuerySideData>> {
    let context = action.context;
    const filter = action.derivedResourceUnidentified.filter;
    const parameters = extractTemplateParams(action.derivedResourceUnidentified.template);

    // General triple pattern queries require a template
    if (parameters.size === 0) {
      return failTest(`${this.name} requires parameters in template of derived resource`);
    }

    // Remove template values and add variables in their place so we can parse the query
    const { normalized } = normalizeQuery(filter, parameters);

    let queryParseOutput;
    try {
      const baseIRI: string | undefined = context.get(KeysInitQuery.baseIRI);
      const queryFormat: RDF.QueryFormat = context.get(KeysInitQuery.queryFormat)!;
      queryParseOutput = await this.filterParseCache.parse(
        FilterParseCache.key(normalized, queryFormat, baseIRI),
        async() => this.mediatorQueryParse.mediate({ context, query: normalized, queryFormat, baseIRI }),
      );
    }
    catch (err: any) {
      return failTest(`${this.name} parsing query failed with: ${err.message}`);
    }

    if (!isKnownOperation(queryParseOutput.operation, Algebra.Types.PROJECT)) {
      return failTest(`${this.name} only works with select templates`);
    }

    const operationInput = queryParseOutput.operation.input;
    if (!isKnownOperation(operationInput, Algebra.Types.BGP)) {
      return failTest(`${this.name} requires a WHERE clause with only a BGP`);
    }

    const linearPatterns = findLinearOrder(operationInput.patterns);
    if (!linearPatterns) {
      return failTest(`${this.name} requires a path-shaped query chaining distinct variables`);
    }

    if (!this.selectsLinearVariables(linearPatterns, queryParseOutput.operation.variables)) {
      return failTest(`${this.name} requires the select clause to project all path variables`);
    }

    if (!(parameters.size >= linearPatterns.length) ||
      !this.allPredicatesParameters(linearPatterns, parameters) ||
      linearPatterns.length !== new Set(linearPatterns.map(p => p.predicate.value)).size
    ) {
      return failTest(`${this.name} requires exclusively predicate parameters without repeats`);
    }

    return passTestVoidWithSideData<ILinearQuerySideData>({
      parameters,
      operation: queryParseOutput.operation,
    });
  }

  public async run(
    action: IActionDerivedResourceIdentify,
    sideData: ILinearQuerySideData,
  ): Promise<IActorDerivedResourceIdentifyOutput> {
    const templateString = new URL(
      action.derivedResourceUnidentified.template,
      action.derivedResourceUnidentified.baseUrl
    ).href;

    const proxySource = new QuerySourceParameterizedLinearQuery(
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
        resourceCoefficients: {
          selectivity: 1,
          requests: 1,
          compute: 5
        }
      }
    }
    return derivedResource;
  }

  /**
   * Checks whether all variables of the path are projected, as the derived resource
   * can only return bindings for the variables its query selects.
   * @param patterns The patterns of the path, in path order.
   * @param variables The variables projected by the query of the derived resource.
   */
  public selectsLinearVariables(patterns: Algebra.Pattern[], variables: RDF.Variable[]): boolean {
    const varSet = new Set(variables.map(v => v.value));
    if (!varSet.has(patterns[0].subject.value)) {
      return false;
    }
    return patterns.every(pattern => varSet.has(pattern.object.value));
  }

  public allPredicatesParameters(patterns: Algebra.Pattern[], parameters: Set<string>) {
    return patterns.every((pattern) =>
      parameters.has(pattern.predicate.value.replace('__param_', ''))
    )
  }

  public extractTemplateParams(templateStr: string) {
    return extractTemplateParams(templateStr);
  }

  public normalizeQuery(rawQuery: string, paramNames: Set<string>) {
    return normalizeQuery(rawQuery, paramNames);
  }
}

export interface IActorDerivedResourceIdentifyLinearQueryArgs
  extends IActorDerivedResourceIdentifyArgs<ILinearQuerySideData> {
  mediatorDereference: MediatorDereference;
  mediatorQueryParse: MediatorQueryParse;
}

export interface ILinearQuerySideData {
  parameters: Set<string>;
  operation: Algebra.Project;
}
