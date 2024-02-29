import type { ILink } from '@comunica/bus-rdf-resolve-hypermedia-links';
import type { IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Actor } from '@comunica/core';
import { EdgeListGraph } from '@comunica/actor-construct-traversed-topology-graph-based-prioritisation/lib/EdgeListGraph';

// TODO:
// this is currently too tightly coupled with the actor implementation, instead this shoudl define 
// an abstract class that represents tracked topology, this class should define methods like: putNode, updateNode, getNodes, getMetadata, etc

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
  topology: EdgeListGraph
}

export type IActorConstructTraversedTopologyArgs = IActorArgs<
IActionConstructTraversedTopology, IActorTest, IActorConstructTraversedTopologyOutput>;

export type MediatorConstructTraversedTopology = Mediate<
IActionConstructTraversedTopology, IActorConstructTraversedTopologyOutput>;
