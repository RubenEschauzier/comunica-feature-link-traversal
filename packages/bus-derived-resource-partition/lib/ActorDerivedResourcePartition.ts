import { IDerivedResource } from '@comunica/actor-extract-links-solid-derived-resources';
import { Actor, IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Algebra } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';
import { MediatorDerivedResourcePropose } from '@comunica/bus-derived-resource-propose';

/**
 * A comunica actor for derived-resource-partition events. This
 * first gets for identified derived resources all possible execution sub-queries.
 * Then it solves any overlap issues by partitioning the execution to the 
 * derived resources expected to be most efficient.
 *
 * Actor types:
 * * Input:  IActionDerivedResourcePartition:      TODO: fill in.
 * * Test:   <none>
 * * Output: IActorDerivedResourcePartitionOutput: TODO: fill in.
 *
 * @see IActionDerivedResourcePartition
 * @see IActorDerivedResourcePartitionOutput
 */
export abstract class ActorDerivedResourcePartition<TS = undefined> 
  extends Actor<IActionDerivedResourcePartition, IActorDerivedResourcePartitionTest, IActorDerivedResourcePartitionOutput, TS> {
  public readonly mediatorDerivedResourcePropose: MediatorDerivedResourcePropose;
  /**
  * @param args -
   *   \ @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   *   \ @defaultNested {TODO failed: none of the configured actors were to TODO} busFailMessage
  */
  public constructor(args: IActorDerivedResourcePartitionArgs<TS>) {
    super(args);
    this.mediatorDerivedResourcePropose = args.mediatorDerivedResourcePropose;
  }
}

export interface IActionDerivedResourcePartition extends IAction {
  /**
   * The query being executed
   */
  operation: Algebra.Operation;
  /**
   * The available resources for this run
   */
  resources: IDerivedResource[];
}

export interface IActorDerivedResourcePartitionOutput extends IActorOutput {
  resourceExecutionBlocks: IResourceExecutionBlock[];
}

export interface IResourceExecutionBlock {
  operation: Algebra.Operation[];
  resource: IDerivedResource;
  anchors: RDF.Term[];
  prunable: boolean;
}

export interface IActorDerivedResourcePartitionTest extends IActorTest{
  accuracy: number;
}

export interface IActorDerivedResourcePartitionArgs<TS = undefined> extends IActorArgs<
  IActionDerivedResourcePartition,
  IActorDerivedResourcePartitionTest,
  IActorDerivedResourcePartitionOutput,
  TS
> {
  mediatorDerivedResourcePropose: MediatorDerivedResourcePropose;
}


export type MediatorDerivedResourcePartition = Mediate<
IActionDerivedResourcePartition, IActorDerivedResourcePartitionOutput>;
