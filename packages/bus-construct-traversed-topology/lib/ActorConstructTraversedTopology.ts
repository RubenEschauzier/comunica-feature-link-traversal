import type { ILink } from '@comunica/bus-rdf-resolve-hypermedia-links';
import type { IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Actor } from '@comunica/core';
import { Topology } from './Topology';
/**
 * A comunica actor for construct-traversed-topology events.
 *
 * Actor types:
 * * Input:  IActionConstructTraversedTopology:      TODO: fill in.
 * * Test:   <none>
 * * Output: IActorConstructTraversedTopologyOutput: TODO: fill in.
 *
 * @see IActionConstructTraversedTopology
 * @see IActorConstructTraversedTopologyOutput
 */
export abstract class ActorConstructTraversedTopology extends Actor<IActionConstructTraversedTopology, IActorTest, IActorConstructTraversedTopologyOutput> {
  /**
  * @param args - @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
  */
  public constructor(args: IActorConstructTraversedTopologyArgs) {
    super(args);
  }
}

export interface IActionConstructTraversedTopology extends IAction {
  /**
   * The url of the document used to extract new links
   */
  parentUrl: string;
  /**
   * The extracted links
   */
  links: ILink[];
  /**
   * Extracted links metadata
   */
  metadata: Record<string, any>[];
  /**
   * Whether we add node or update metadata of node 
   */
  setDereferenced: boolean;
}

export interface IActorConstructTraversedTopologyOutput extends IActorOutput {
  topology: Topology
}

export type IActorConstructTraversedTopologyArgs = IActorArgs<
IActionConstructTraversedTopology, IActorTest, IActorConstructTraversedTopologyOutput>;

export type MediatorConstructTraversedTopology = Mediate<
IActionConstructTraversedTopology, IActorConstructTraversedTopologyOutput>;
