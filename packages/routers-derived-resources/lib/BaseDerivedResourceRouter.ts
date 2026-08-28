import type { Bindings } from '@comunica/utils-bindings-factory';

import type * as RDF from '@rdfjs/types';
import { 
  stemsContextKeys, 
  IStemsRouter, 
  IStemsRoutingEntry, 
  RouterBase,
  HashFunction,
  ITimestampGenerator,
  JoinFunction
} from '@comunica/actor-rdf-join-inner-multi-stems';
import { StemsOperatorStream } from '@comunica/actor-rdf-join-inner-multi-stems';
import { IDerivedResource } from '@comunica/actor-extract-links-solid-derived-resources';
import { Algebra } from '@comunica/utils-algebra';
import equal from 'deep-equal';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';

// TODO: Maybe make optional to only update certain routing table entries
export abstract class RouterBaseDerivedResource 
  extends RouterBase implements IEddiesRouterDerivedResource {
  
  public addDerivedResource(
    derivedOperations: Algebra.Operation[],
    derivedResource: IDerivedResource, 
    routeTable: Record<number, IStemsRoutingEntry[][]>,
    timestampGenerator: ITimestampGenerator,
    hashFn: HashFunction,
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
    

    // Determine what bits in mask are answered by derived resource
    const setBitsMask = this.getDerivedResourceBits(derivedOperations);

    // TODO: What should derivedOperations be, probably a regular Algebra.Operation not an array 
    // TODO: We need to convert the derived resource into bindings, which depends on the other variables
    // the derived resource should join over.
    // TODO: Determine metadata
    // TODO: How do we expose new operator stream to the existing controller stream, context entry mapping
    // operations to controller?
    
    // Create a new operator stream from the derived resource
    const derivedResourceStemOperator = new StemsOperatorStream(
      entry.output.bindingsStream,
      timestampGenerator,
      hashFn,
      <JoinFunction> ActorRdfJoin.joinBindings,
      this.routeOperations.length,
      setBitsMask,
      entry.operation,
      (await entry.output.metadata()).variables.map(x => x.variable),
      this.getComponentSubjectIRIs(entry),
      entriesJoinVariables[i],
      false,
    );

    for (const [ doneKey, routing ] of Object.entries(routeTable)) {
      const key = Number.parseInt(doneKey, 10);
      // If the done signature has no overlap with the current entry we can route to this derived resource
      if ((key & setBitsMask) === 0){
        const newRouting = []
        // TODO Iterate over all the other operators in the table and determine if there is any overlap with current
        // doneKey +  doneKey of derived resource
      }
      // TODO, do we need to update existing routing?
    }


    // TODO How to? (In the operator stream) Add an eddies operator to controller stream.

    return routeTable;
  }

  protected getDerivedResourceBits(derivedOperations: Algebra.Operation[]){
    const indexes = this.routeOperations.flatMap((routeOperation, idx) =>
      derivedOperations.some(derivedOperation =>
        equal(derivedOperation, routeOperation.operation)
      ) ? [idx] : []
    );
    if (indexes.length === 0){
      throw new Error("Tried to add derived resource with no overlap with current routingTable");
    }
    return this.doneIndexesToMask(indexes);
  }

}

export interface IEddiesRouterDerivedResource extends IStemsRouter {
  addDerivedResource: (
    patterns: Algebra.Operation[],
    derivedResource: IDerivedResource, 
    routingTable: Record<number, IStemsRoutingEntry[][]>,
  ) => Record<number, IStemsRoutingEntry[][]>;
}
