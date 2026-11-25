import { ICompositeExecutionBlock, IResourceExecutionBlock, ITriplePatternExecutionBlock } from '@comunica/bus-derived-resource-partition';
import { Actor, IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { ILink } from '@comunica/types';
import type { CompositeResponseGate } from './CompositeResponseGate';

/**
 * A comunica actor for derived-resource-execute events. This runs the work that the partition actor
 * assigned to derived resources.
 *
 * Which resources answer which part of the query is decided by the propose and partition buses, so
 * an actor here only has to run the blocks of the kind it knows how to run.
 *
 * Actor types:
 * * Input:  IActionDerivedResourceExecute:      The blocks to run.
 * * Test:   <none>
 * * Output: IActorDerivedResourceExecuteOutput: The links discovered while running them.
 *
 * @see IActionDerivedResourceExecute
 * @see IActorDerivedResourceExecuteOutput
 */
export abstract class ActorDerivedResourceExecute<TS = undefined>
  extends Actor<IActionDerivedResourceExecute, IActorTest, IActorDerivedResourceExecuteOutput, TS> {
  /**
  * @param args -
   *   \ @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   *   \ @defaultNested {TODO failed: none of the configured actors were to TODO} busFailMessage
  */
  public constructor(args: IActorDerivedResourceExecuteArgs<TS>) {
    super(args);
  }

  /**
   * Identifies patterns containing wildcard characters.
   */
  protected isGlob(pattern: string): boolean {
    return pattern.includes('*') || pattern.includes('?');
  }

  /**
   * The blocks that hand a whole set of join entries to one resource. A block of one operation is left out:
   * it offloads no join, being the request a pattern-level resource makes as well, so its pattern is answered
   * as any pattern no composite answers rather than fetched twice.
   */
  protected compositeBlocks(action: IActionDerivedResourceExecute): ICompositeExecutionBlock[] {
    return action.executionBlocks.filter(
      (block): block is ICompositeExecutionBlock => block.type === 'composite' && block.operations.length > 1,
    );
  }

  /**
   * The blocks that serve a single pattern from a resource rather than crawling for it.
   */
  protected triplePatternBlocks(action: IActionDerivedResourceExecute): ITriplePatternExecutionBlock[] {
    return action.executionBlocks.filter(
      (block): block is ITriplePatternExecutionBlock => block.type === 'triple-pattern',
    );
  }
}

export interface IActionDerivedResourceExecute extends IAction {
  /**
   * The work the partition actor assigned to derived resources. Composite blocks are disjoint over
   * their operations, so every actor on this bus can run the blocks of its own kind without
   * coordinating with the others.
   */
  executionBlocks: IResourceExecutionBlock[];
  /**
   * Opens once the composite requests of this execution have been answered, for work that can
   * wait until the answers that matter most are in.
   */
  compositeResponses?: CompositeResponseGate;
}

export interface IActorDerivedResourceExecuteOutput extends IActorOutput {
  /**
   * Any new links discovered while running the blocks
   */
  links: ILink[];
}

export type IActorDerivedResourceExecuteArgs<TS = undefined> = IActorArgs<
IActionDerivedResourceExecute, IActorTest, IActorDerivedResourceExecuteOutput, TS>;

export type MediatorDerivedResourceExecute = Mediate<
IActionDerivedResourceExecute, IActorDerivedResourceExecuteOutput>;
