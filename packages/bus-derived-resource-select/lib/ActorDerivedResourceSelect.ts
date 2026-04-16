import { IDerivedResource } from '@comunica/actor-extract-links-solid-derived-resources';
import { Actor, IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';

/**
 * A comunica actor for derived-resource-select events.
 *
 * Actor types:
 * * Input:  IActionDerivedResourceSelect:      TODO: fill in.
 * * Test:   <none>
 * * Output: IActorDerivedResourceSelectOutput: TODO: fill in.
 *
 * @see IActionDerivedResourceSelect
 * @see IActorDerivedResourceSelectOutput
 */
export abstract class ActorDerivedResourceSelect<
TS extends IActorDerivedResourceSelectTestSideData = IActorDerivedResourceSelectTestSideData
> 
extends Actor<IActionDerivedResourceSelect, IActorTest, IActorDerivedResourceSelectOutput, TS> {
  /**
  * @param args -
   *   \ @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   *   \ @defaultNested {TODO failed: none of the configured actors were to TODO} busFailMessage
  */
  public constructor(args: IActorDerivedResourceSelectArgs<TS>) {
    super(args);
  }

  /**
   * Determines if a derived resource is usable for this actor
   */
  public abstract hasRequiredResources(
    derivedResources: IDerivedResource[],
    action: IActionDerivedResourceSelect,
  ): IRequiredResources;
}

export interface IActionDerivedResourceSelect extends IAction {
  derivedResourcesIdentified: IDerivedResource[];
}

export interface IActorDerivedResourceSelectOutput extends IActorOutput {

}

export interface IActorDerivedResourceSelectTestSideData {
  usableResources: IDerivedResource[];
}

export interface IRequiredResources {
  /**
   * Whether the identified resources in a data source are sufficient
   * to execute the actor's operation.
   */
  canAnswer: boolean, 
  /**
   * The set of resources that can be used to execute this operation.
   */
  usableResources: Set<IDerivedResource>
}

export type IActorDerivedResourceSelectArgs<
TS extends IActorDerivedResourceSelectTestSideData = IActorDerivedResourceSelectTestSideData
> = IActorArgs<
IActionDerivedResourceSelect, IActorTest, IActorDerivedResourceSelectOutput, TS>;

export type MediatorDerivedResourceSelect = Mediate<
IActionDerivedResourceSelect, IActorDerivedResourceSelectOutput>;
