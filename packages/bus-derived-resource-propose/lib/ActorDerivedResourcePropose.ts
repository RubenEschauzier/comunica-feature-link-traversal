import { IDerivedResource, IDerivedResourceCoefficients } from '@comunica/actor-extract-links-solid-derived-resources';
import { Actor, IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Algebra } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';

/**
 * A comunica actor for derived-resource-propose events.
 *
 * Actor types:
 * * Input:  IActionDerivedResourcePropose:      TODO: fill in.
 * * Test:   <none>
 * * Output: IActorDerivedResourceProposeOutput: TODO: fill in.
 *
 * @see IActionDerivedResourcePropose
 * @see IActorDerivedResourceProposeOutput
 */
export abstract class ActorDerivedResourcePropose<TS = undefined> extends Actor<IActionDerivedResourcePropose, IActorTest, IActorDerivedResourceProposeOutput, TS> {
  /**
  * @param args -
   *   \ @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   *   \ @defaultNested {TODO failed: none of the configured actors were to TODO} busFailMessage
  */
  public constructor(args: IActorDerivedResourceProposeArgs<TS>) {
    super(args);
  }
}

export interface IActionDerivedResourcePropose extends IAction {
  operation: Algebra.Operation;
  resources: IDerivedResource[];
}

export interface IActorDerivedResourceProposeOutput extends IActorOutput {
  candidateResources: ICandidateResource[];
}

export interface ICandidateResource {
  /**
   * Operations this resource covers
   */
  operations: Algebra.Operation[];
  /**
   * The associated resource to execute the sub-query
   */
  resource: IDerivedResource;
  /**
   * The type of sub-query executed
   */
  kind: "star"| "chain" | string;
  /**
   * The terms needed to be in authoritative domain of tihs resource
   */
  anchorTerms: RDF.Term[];
  /**
   * If using this resources allows us to prune work in later query execution
  */
  prunable: boolean;
  features: {
    patternCount: number;
    constantCount: number;
    coefficients: IDerivedResourceCoefficients;
  }
}

export type IActorDerivedResourceProposeArgs<TS = undefined> = IActorArgs<
IActionDerivedResourcePropose, IActorTest, IActorDerivedResourceProposeOutput, TS>;

export type MediatorDerivedResourcePropose = Mediate<
IActionDerivedResourcePropose, IActorDerivedResourceProposeOutput>;
