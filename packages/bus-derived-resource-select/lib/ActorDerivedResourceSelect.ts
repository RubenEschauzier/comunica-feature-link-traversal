import { IDerivedResource } from '@comunica/actor-extract-links-solid-derived-resources';
import { Actor, IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { IActionContext } from '@comunica/types';

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
   * Converts a standard glob pattern to a RegExp instance.
   */
  protected globToRegExp(glob: string): RegExp {
    // const strippedGlob = this.stripExtensionFromGlob(glob);
    const escaped = glob.replace(/[.+^${}()|[\]\\]/g, '\\$&');
    const regexString = `^${escaped.replace(/\*/g, '.*').replace(/\?/g, '.')}$`;
    
    return new RegExp(regexString);
  }
  /**
   * Removes literal alphanumeric file extensions from a glob pattern.
   * Preserves wildcard patterns (e.g., '.*') and paths without extensions.
   */
  protected stripExtensionFromGlob(glob: string): string {
    // Matches a literal dot followed by one or more alphanumeric characters at the string's end.
    return glob.replace(/\.[a-zA-Z0-9]+$/, '');
  }
  /**
   * Identifies patterns containing wildcard characters.
   */
  protected isGlob(pattern: string): boolean {
    return pattern.includes('*') || pattern.includes('?');
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
  derivedResourceContext: IActionContext;
}

export interface IRequiredResources {
  /**
   * Whether the identified resources in a data source are sufficient
   * to execute the actor's operation.
   */
  canAnswer: boolean;
  /**
   * The set of resources that can be used to execute this operation.
   */
  usableResources: Set<IDerivedResource>;
  /**
   * Context object for passing derived resource-specific information
   */
  derivedResourceContext: IActionContext;
}

export type IActorDerivedResourceSelectArgs<
TS extends IActorDerivedResourceSelectTestSideData = IActorDerivedResourceSelectTestSideData
> = IActorArgs<
IActionDerivedResourceSelect, IActorTest, IActorDerivedResourceSelectOutput, TS>;

export type MediatorDerivedResourceSelect = Mediate<
IActionDerivedResourceSelect, IActorDerivedResourceSelectOutput>;
