import {
  ActorQuerySourceDereferenceLink,
} from '@comunica/bus-query-source-dereference-link';
import type {
  IActionQuerySourceDereferenceLink,
  IActorQuerySourceDereferenceLinkOutput,
  IActorQuerySourceDereferenceLinkArgs,
  MediatorQuerySourceDereferenceLink,
} from '@comunica/bus-query-source-dereference-link';
import type { IActorRdfMetadataOutput, MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import type { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import type { TestResult, IActorTest } from '@comunica/core';
import { ActionContextKey, failTest, passTestVoid } from '@comunica/core';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import { CacheEntrySourceState } from '@comunica/cache-manager-entries';
import { CacheSourceStateView } from '@comunica/cache-manager-entries/lib/ViewKeys';
import { BindingsFactory } from '@comunica/utils-bindings-factory';

import type * as RDF from '@rdfjs/types';
import { IActionContext, ILink, ISourceState } from '@comunica/types';
import { KeysCore, KeysQueryOperation } from '@comunica/context-entries';
import { DataFactory } from 'rdf-data-factory';
import { Factory } from 'sparqlalgebrajs';

/**
 * A comunica Dereference Query Source Hypermedia Resolve Actor.
 */
export class ActorQuerySourceDereferenceLinkHypermediaWrapCache extends ActorQuerySourceDereferenceLink {
  public readonly mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;
  public readonly mediatorMetadata: MediatorRdfMetadata;
  public readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;

  public readonly DF: DataFactory = new DataFactory();
  public readonly BF: BindingsFactory = new BindingsFactory(this.DF, {});
  public readonly AF: Factory = new Factory(this.DF);

  public constructor(args: IActorQuerySourceDereferenceLinkHypermediaWrapCacheArgs) {
    super(args);
    this.mediatorQuerySourceDereferenceLink = args.mediatorQuerySourceDereferenceLink;
    this.mediatorMetadata = args.mediatorMetadata;
    this.mediatorMetadataExtract = args.mediatorMetadataExtract;
  }

  public async test(action: IActionQuerySourceDereferenceLink): Promise<TestResult<IActorTest>> {
    if (action.context.get(KEY_WRAPPED)){
      return failTest("Can only wrap dereference link once");
    }
    return passTestVoid();
  }

  public async run(action: IActionQuerySourceDereferenceLink): Promise<IActorQuerySourceDereferenceLinkOutput> {
    let context = action.link.context ? action.context.merge(action.link.context) : action.context;
    
    const cacheManager = context.getSafe(KeysCaching.cacheManager);

    let sourceFromCache: ISourceState | undefined;
    try {
      sourceFromCache = await cacheManager.getFromCache(
          CacheEntrySourceState.cacheSourceState,
          CacheSourceStateView.cacheSourceStateView,
          { url: action.link.url }
      );
    } catch(err: any) {
      action.context.get(KeysCore.log)?.error(`Error when getting from cache: ${err.message}`);
      throw err;
    }
    if (sourceFromCache && sourceFromCache.cachePolicy?.satisfiesWithoutRevalidation(action)){
        console.log("Using cached!");
        // Re-extract traverse metadata so the followed links are up-to-date with current
        // query
        const traverse = this.reExtractTraverseMetadata(sourceFromCache, action.link.url, context);
        console.log(traverse);
        sourceFromCache.metadata.traverse = traverse;
        // Return the source but set storable to false. This way the response won't be stored
        // multiple times
        sourceFromCache.cachePolicy = { ...sourceFromCache.cachePolicy, storable: () => false }
        return sourceFromCache;
    }
    action.context = action.context.set(KEY_WRAPPED, true);
    const dereferenceLinkOutput = await this.mediatorQuerySourceDereferenceLink.mediate(action);

    await cacheManager.setCache(
      CacheEntrySourceState.cacheSourceState,
      action.link.url,
      { link: action.link, handledDatasets: action.handledDatasets!, ...dereferenceLinkOutput },
      { headers: dereferenceLinkOutput.headers }
    );

    if (dereferenceLinkOutput.cachePolicy){
        dereferenceLinkOutput.cachePolicy = { ...dereferenceLinkOutput.cachePolicy, storable: () => false };
    }
    return dereferenceLinkOutput;
  }

  protected async reExtractTraverseMetadata(source: ISourceState, url: string, context: IActionContext){
    const quads = <RDF.Stream> source.source.queryBindings(
        this.AF.createPattern(
            this.DF.variable('s'),
            this.DF.variable('p'),
            this.DF.variable('o'),
            this.DF.variable('g'),
        ),
        context.set(KeysQueryOperation.unionDefaultGraph, true),
        ).map(bindings => (<RDF.DataFactory<RDF.BaseQuad>> this.DF).quad(
            bindings.get('s')!,
            bindings.get('p')!,
            bindings.get('o')!,
            bindings.get('g'),
        ));
    // Determine the metadata (TODO: We set triples to false, but this should probably be in the metadata
    // of the stored ISourceState so we don't have to just assume its not quads or does it not matter
    // for traverse value)
    const rdfMetadataOutputCachedSource: IActorRdfMetadataOutput = await this.mediatorMetadata.mediate(
    { context, url, quads, triples: false },
    );

    rdfMetadataOutputCachedSource.data.on('error', () => {
    // Silence errors in the data stream,
    // as they will be emitted again in the metadata stream,
    // and will result in a promise rejection anyways.
    // If we don't do this, we end up with an unhandled error message
    });

    const metadataReExtract = (await this.mediatorMetadataExtract.mediate({
        context,
        url,
        metadata: rdfMetadataOutputCachedSource.metadata,
        headers: source.headers,
        requestTime: 0,
    })).metadata;

    // TODO: CHeck if with headers we also extract the .meta files now?
    const traverseDotMetaOnly = source.metadata.traverse.filter(
        (link: ILink) => link.url.endsWith('.meta'),
    );
    const traverseNew = [ ...traverseDotMetaOnly, ...metadataReExtract.traverse ];
    return traverseNew;
  }
}

export interface IActorQuerySourceDereferenceLinkHypermediaWrapCacheArgs extends IActorQuerySourceDereferenceLinkArgs {
  mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink
  mediatorMetadata: MediatorRdfMetadata;
  mediatorMetadataExtract: MediatorRdfMetadataExtract;
}

export const KEY_WRAPPED = new ActionContextKey<boolean>(
    '@comunica/query-source-dereference-link-hypermedia-wrap-cache:wrapped'
);