import type { Bindings } from '@comunica/utils-bindings-factory';

import type * as RDF from '@rdfjs/types';
import { 
  stemsContextKeys, 
  IStemsRouter, 
  IStemsRoutingEntry, 
  RouterBase
} from '@comunica/actor-rdf-join-inner-multi-stems';
import type { StemsOperatorStream } from '@comunica/actor-rdf-join-inner-multi-stems';
import { IDerivedResource } from '@comunica/actor-extract-links-solid-derived-resources';
import { Algebra } from '@comunica/utils-algebra';

// TODO: Maybe make optional to only update certain routing table entries
export abstract class RouterBaseDerivedResource 
  extends RouterBase implements IEddiesRouterDerivedResource {
  
  public addDerivedResource(
    patterns: Algebra.Pattern[],
    derivedResource: IDerivedResource, 
    routingTable: Record<number, IStemsRoutingEntry[][]>
  ): Record<number, IStemsRoutingEntry[][]> {
    // Steps: 
    // 0.5 (Optional) update existing routing table.
    // 1. Determine the set bits of a derived resource. 
    // (requires the bgp and the patterns answered in resource)
    // 2. For each signature in table determine if you can use the given derived resource 
    // (by comparing signatures)
    // 3. If it can, add a routing table for that derived resource to the IStemsRoutingEntry[][] 
    // by appending. We can say to start with the derived resource in those cases to obtain data on
    // tickets

    // (In the operator stream) Add an eddies operator to controller stream.

    return routingTable;
  }
}

export interface IEddiesRouterDerivedResource extends IStemsRouter {
  addDerivedResource: (
    patterns: Algebra.Pattern[],
    derivedResource: IDerivedResource, 
    routingTable: Record<number, IStemsRoutingEntry[][]>,
  ) => Record<number, IStemsRoutingEntry[][]>;

}
