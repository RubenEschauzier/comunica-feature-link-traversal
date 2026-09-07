import type { MediatorDereference } from '@comunica/bus-dereference';
import { QuerySourceParameterizedQuery } from '@comunica/bus-derived-resource-identify';
import type { ComunicaDataFactory } from '@comunica/types';
import type { Algebra } from '@comunica/utils-algebra';
import { findLinearOrder } from './LinearShape';

/**
 * A query source over a linear (path-shaped) derived resource, of which the linear shape is
 * validated by ActorDerivedResourceIdentifyLinearQuery.
 */
export class QuerySourceParameterizedLinearQuery extends QuerySourceParameterizedQuery {
  public constructor(
    template: string,
    operation: Algebra.Project,
    parameters: Set<string>,
    mediatorDereference: MediatorDereference,
    dataFactory: ComunicaDataFactory,
  ) {
    // Unlike a star, the order of the patterns of a path decides which parameter of the template
    // each pattern fills, so both the resource and every incoming BGP are put in path order
    super(
      template,
      operation,
      parameters,
      mediatorDereference,
      dataFactory,
      'linear query source',
      patterns => findLinearOrder(patterns) ?? patterns,
    );
  }
}
