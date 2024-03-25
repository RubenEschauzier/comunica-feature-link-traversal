import { RdfJsQuadSource } from '@comunica/actor-rdf-resolve-quad-pattern-rdfjs-source';
import { IActionConstructTraversedTopology, MediatorConstructTraversedTopology } from '@comunica/bus-construct-traversed-topology';
import type {
  IActionRdfResolveHypermedia, IActorRdfResolveHypermediaArgs,
  IActorRdfResolveHypermediaOutput, IActorRdfResolveHypermediaTest,
} from '@comunica/bus-rdf-resolve-hypermedia';
import { ActorRdfResolveHypermedia } from '@comunica/bus-rdf-resolve-hypermedia';
import { KeysTraversedTopology } from '@comunica/context-entries-link-traversal';
import { ActionContext } from '@comunica/core';
import { storeStream } from 'rdf-store-stream';
import { RdfStore } from 'rdf-stores'

/**
 * A comunica None RDF Resolve Hypermedia Actor.
 */
export class ActorRdfResolveHypermediaNone extends ActorRdfResolveHypermedia {
  public constructor(args: IActorRdfResolveHypermediaArgs) {
    super(args, 'file');
  }

  public async testMetadata(action: IActionRdfResolveHypermedia): Promise<IActorRdfResolveHypermediaTest> {
    return { filterFactor: 0 };
  }

  public async run(action: IActionRdfResolveHypermedia): Promise<IActorRdfResolveHypermediaOutput> {
    this.logInfo(action.context, `Identified as file source: ${action.url}`);
    const source = <RdfStore> await storeStream(action.quads);
    // Set size of source as weight in topology that tracks document size
    const mediatorConstructTraversedTopology = <MediatorConstructTraversedTopology> 
    action.context.get(KeysTraversedTopology.mediatorConstructTraversedTopology);
    const actionUpdateTopology: IActionConstructTraversedTopology = {
      parentUrl: '',
      links: [{url: action.url}],
      metadata: [{weightDocumentSize: source.size}],
      context: new ActionContext({}),
      setDereferenced: true
    }  
    await mediatorConstructTraversedTopology.mediate(actionUpdateTopology);

    return { source: new RdfJsQuadSource(source) };
  }
}
