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

export type IResourceExecutionBlock = ICompositeExecutionBlock | ITriplePatternExecutionBlock;

/**
 * A conjunctive set of join entries that one resource answers as a whole, to be handed to the
 * adaptive join as a composite source.
 */
export interface ICompositeExecutionBlock {
  type: 'composite';
  /**
   * The join entries this resource answers, matched against a component one by one
   */
  operations: Algebra.Operation[];
  resource: IDerivedResource;
  /**
   * The terms that have to be in the authoritative domain of the resource
   */
  anchorTerms: RDF.Term[];
}

/**
 * Triple pattern-level delegation to a derived resource
 */
export interface ITriplePatternExecutionBlock {
  type: 'triple-pattern';
  pattern: Algebra.Pattern;
  resources: IDerivedResource[];
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
