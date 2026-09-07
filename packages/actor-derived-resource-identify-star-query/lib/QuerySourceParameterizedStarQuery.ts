import type { MediatorDereference } from '@comunica/bus-dereference';
import { QuerySourceParameterizedQuery } from '@comunica/bus-derived-resource-identify';
import type { ComunicaDataFactory } from '@comunica/types';
import type { Algebra } from '@comunica/utils-algebra';

/**
 * A query source over a star-shaped derived resource, of which the star-shape is
 * validated by ActorDerivedResourceIdentifyStarQuery.
 */
export class QuerySourceParameterizedStarQuery extends QuerySourceParameterizedQuery {
  public constructor(
    template: string,
    operation: Algebra.Project,
    parameters: Set<string>,
    mediatorDereference: MediatorDereference,
    dataFactory: ComunicaDataFactory,
  ) {
    super(template, operation, parameters, mediatorDereference, dataFactory, 'star query source');
  }
}
