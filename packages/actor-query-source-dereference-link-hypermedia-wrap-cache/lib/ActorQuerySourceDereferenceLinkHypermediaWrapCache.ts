import {
  ActorQuerySourceDereferenceLink,
} from '@comunica/bus-query-source-dereference-link';
import type {
  IActionQuerySourceDereferenceLink,
  IActorQuerySourceDereferenceLinkOutput,
  IActorQuerySourceDereferenceLinkArgs,
  MediatorQuerySourceDereferenceLink,
} from '@comunica/bus-query-source-dereference-link';
import type { TestResult, IActorTest } from '@comunica/core';
import { ActionContextKey, failTest, passTestVoid } from '@comunica/core';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import { CacheEntrySourceState } from '@comunica/cache-manager-entries';
import { CacheSourceStateView } from '@comunica/cache-manager-entries/lib/ViewKeys';

/**
 * A comunica Dereference Query Source Hypermedia Resolve Actor.
 */
export class ActorQuerySourceDereferenceLinkHypermediaWrapCache extends ActorQuerySourceDereferenceLink {
  public readonly mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;

  public constructor(args: IActorQuerySourceDereferenceLinkHypermediaWrapCacheArgs) {
    super(args);
    this.mediatorQuerySourceDereferenceLink = args.mediatorQuerySourceDereferenceLink;
  }

  public async test(action: IActionQuerySourceDereferenceLink): Promise<TestResult<IActorTest>> {
    if (action.context.get(KEY_WRAPPED)){
        return failTest("Can only wrap derefernce link once");
    }
    return passTestVoid();
  }

  public async run(action: IActionQuerySourceDereferenceLink): Promise<IActorQuerySourceDereferenceLinkOutput> {
    const context = action.link.context ? action.context.merge(action.link.context) : action.context;
    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    
    const sourceFromCache = cacheManager.getFromCache(
        CacheEntrySourceState.cacheSourceState,
        CacheSourceStateView.cacheSourceStateView,
        { url: action.link.url }
    )
    // TODO Check policy and also ensure that it isn't stored as we don't want to double cache
    if (sourceFromCache){
        return sourceFromCache;
    }
    // if (cacheManager.getFromCache(KeysCaching.cacheManager, ))
    // TODO Take from cache here first also if taking cache we need to reextract metadata for traversal
    const dereferenceLinkOutput = await this.mediatorQuerySourceDereferenceLink.mediate(action);

    cacheManager.setCache(
        CacheEntrySourceState.cacheSourceState,
        action.link.url,
        {link: action.link, handledDatasets: action.handledDatasets!, ...dereferenceLinkOutput},
        {url: action.link.url}
    )
    // let url = action.link.url;
    // let quads: RDF.Stream;
    // let metadata: Record<string, any>;
    // let cachePolicy: ICachePolicy<IActionQuerySourceDereferenceLink> | undefined;
    // try {
    //   const dereferenceRdfOutput: IActorDereferenceRdfOutput = await this.mediatorDereferenceRdf
    //     .mediate({ context, url });
    //   url = dereferenceRdfOutput.url;
    //   if (dereferenceRdfOutput.cachePolicy) {
    //     cachePolicy = new QuerySourceCachePolicyDereferenceWrapper(dereferenceRdfOutput.cachePolicy);
    //   }

    //   // Determine the metadata
    //   const rdfMetadataOutput: IActorRdfMetadataOutput = await this.mediatorMetadata.mediate(
    //     { context, url, quads: dereferenceRdfOutput.data, triples: dereferenceRdfOutput.metadata?.triples },
    //   );

    //   rdfMetadataOutput.data.on('error', () => {
    //     // Silence errors in the data stream,
    //     // as they will be emitted again in the metadata stream,
    //     // and will result in a promise rejection anyways.
    //     // If we don't do this, we end up with an unhandled error message
    //   });

    //   metadata = (await this.mediatorMetadataExtract.mediate({
    //     context,
    //     url,
    //     // The problem appears to be conflicting metadata keys here
    //     metadata: rdfMetadataOutput.metadata,
    //     headers: dereferenceRdfOutput.headers,
    //     requestTime: dereferenceRdfOutput.requestTime,
    //   })).metadata;
    //   quads = rdfMetadataOutput.data;

    //   // Transform quads if needed.
    //   if (action.link.transform) {
    //     quads = await action.link.transform(quads);
    //   }
    // } catch (error: unknown) {
    //   // Make sure that dereference errors are only emitted once an actor really needs the read quads
    //   // This allows SPARQL endpoints that error on service description fetching to still be source-forcible
    //   quads = new Readable();
    //   quads.read = () => {
    //     setTimeout(() => quads.emit('error', error));
    //     return null;
    //   };
    //   ({ metadata } = await this.mediatorMetadataAccumulate.mediate({ context, mode: 'initialize' }));

    //   // Log as warning, because the quads above may not always be consumed (e.g. for SPARQL endpoints),
    //   // so the user would not be notified of something going wrong otherwise.
    //   this.logWarn(context, `Metadata extraction for ${action.link.url} failed: ${(<Error>error).message}`);
    // }

    // // Determine the source
    // const { source, dataset } = await this.mediatorQuerySourceIdentifyHypermedia.mediate({
    //   context,
    //   forceSourceType: action.link.forceSourceType,
    //   handledDatasets: action.handledDatasets,
    //   metadata,
    //   quads,
    //   url,
    // });

    // if (dataset && action.handledDatasets) {
    //   // Mark the dataset as applied
    //   // This is needed to make sure that things like QPF search forms are only applied once,
    //   // and next page links are followed after that.
    //   action.handledDatasets[dataset] = true;
    // }

    // // Track dereference event
    // context.get(KeysStatistics.dereferencedLinks)?.updateStatistic({ url: action.link.url, metadata }, source);

    return dereferenceLinkOutput;
  }
}

export interface IActorQuerySourceDereferenceLinkHypermediaWrapCacheArgs extends IActorQuerySourceDereferenceLinkArgs {
    mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink
}

export const KEY_WRAPPED = new ActionContextKey<boolean>(
    '@comunica/query-source-dereference-link-hypermedia-wrap-cache:wrapped'
);