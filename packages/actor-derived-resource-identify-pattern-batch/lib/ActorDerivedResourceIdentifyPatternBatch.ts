import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type {
  IActionDerivedResourceIdentify,
  IActorDerivedResourceIdentifyArgs,
  IActorDerivedResourceIdentifyOutput,
} from '@comunica/bus-derived-resource-identify';
import { ActorDerivedResourceIdentify } from '@comunica/bus-derived-resource-identify';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
import type { ComunicaDataFactory } from '@comunica/types';
import { DataFactory } from 'rdf-data-factory';
import { QuerySourcePatternBatch } from './QuerySourcePatternBatch';

/**
 * A comunica Pattern Batch Derived Resource Identify Actor.
 *
 * Identifies derived resources whose filter is `patterns`: resources that answer several independent
 * quad patterns in one request, taken from the query string. What such a resource can answer is known
 * from the filter alone, so identifying it takes no request.
 */
export class ActorDerivedResourceIdentifyPatternBatch extends ActorDerivedResourceIdentify {
  protected readonly dataFactory: ComunicaDataFactory = new DataFactory();
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  public readonly maxBatchSize: number;

  public constructor(args: IActorDerivedResourceIdentifyPatternBatchArgs) {
    super(args);
    this.mediatorDereferenceRdf = args.mediatorDereferenceRdf;
    this.maxBatchSize = args.maxBatchSize;
  }

  public async test(action: IActionDerivedResourceIdentify): Promise<TestResult<IActorTest>> {
    if (action.derivedResourceUnidentified.filter.trim() !== 'patterns') {
      return failTest(`${this.name} can only identify pattern batch derived resources`);
    }
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourceIdentify): Promise<IActorDerivedResourceIdentifyOutput> {
    const url = new URL(
      action.derivedResourceUnidentified.template,
      action.derivedResourceUnidentified.baseUrl,
    ).href;
    const querySource = new QuerySourcePatternBatch(url, this.maxBatchSize, this.mediatorDereferenceRdf, this.dataFactory);

    return {
      derivedResourceIdentified: {
        iri: url,
        derivedResourceSelectorShape: await querySource.getSelectorShape(),
        ...action.derivedResourceUnidentified,
        querySource,
        resourceCoefficients: {
          selectivity: 1,
          // Its requests are shared with the other patterns of the pod, so each pattern costs a
          // fraction of one: below the single pattern templates, which are chosen otherwise
          requests: 0.5,
          compute: 1,
        },
      },
    };
  }
}

export interface IActorDerivedResourceIdentifyPatternBatchArgs extends IActorDerivedResourceIdentifyArgs {
  /**
   * The Dereference RDF mediator
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * The most patterns sent in one request, which the server has to accept as well
   * @range {integer}
   * @default {20}
   */
  maxBatchSize: number;
}
