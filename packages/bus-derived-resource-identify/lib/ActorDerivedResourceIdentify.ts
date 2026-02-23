import { Actor, IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { IDerivedResource, IDerivedResourceUnidentified } from '@comunica/actor-extract-links-solid-derived-resources';

/**
 * A comunica actor for derived-resource-identify events.
 *
 * Actor types:
 * * Input:  IActionDerivedResourceIdentify:      TODO: fill in.
 * * Test:   <none>
 * * Output: IActorDerivedResourceIdentifyOutput: TODO: fill in.
 *
 * @see IActionDerivedResourceIdentify
 * @see IActorDerivedResourceIdentifyOutput
 */
export abstract class ActorDerivedResourceIdentify<TS = undefined> extends Actor<IActionDerivedResourceIdentify, IActorTest, IActorDerivedResourceIdentifyOutput, TS> {
  /**
  * @param args -
   *   \ @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   *   \ @defaultNested {TODO failed: none of the configured actors were to TODO} busFailMessage
  */
  public constructor(args: IActorDerivedResourceIdentifyArgs<TS>) {
    super(args);
  }

  public translateFilterIntoSelectorShape(){

  }
}

export interface IActionDerivedResourceIdentify extends IAction {
  derivedResourceUnidentified: IDerivedResourceUnidentified;
}

export interface IActorDerivedResourceIdentifyOutput extends IActorOutput {
  derivedResourceIdentified: IDerivedResource;
}

export type IActorDerivedResourceIdentifyArgs<TS = undefined> = IActorArgs<
IActionDerivedResourceIdentify, IActorTest, IActorDerivedResourceIdentifyOutput, TS>;

export type MediatorDerivedResourceIdentify = Mediate<
IActionDerivedResourceIdentify, IActorDerivedResourceIdentifyOutput>;
