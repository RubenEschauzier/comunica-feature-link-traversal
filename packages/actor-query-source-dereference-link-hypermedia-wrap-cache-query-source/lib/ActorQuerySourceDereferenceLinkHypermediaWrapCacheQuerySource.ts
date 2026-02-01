import { QuerySourceCacheWrapper } from '@comunica/actor-context-preprocess-set-cache-source-state-num-triples';
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
import { CacheEntrySourceState } from '@comunica/cache-manager-entries';
import { CacheSourceStateViews } from '@comunica/cache-manager-entries/lib/ViewKeys';
import { KeysCore, KeysQueryOperation } from '@comunica/context-entries';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import type { TestResult, IActorTest } from '@comunica/core';
import { ActionContext, ActionContextKey, failTest, passTestVoid } from '@comunica/core';
import type { IActionContext, ISourceState } from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';

import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import { Factory } from 'sparqlalgebrajs';
import { QuerySourceStub } from './QuerySourceStub';

/**
 * A comunica Hypermedia Wrap Cache Query Source Query Source Dereference Link Actor.
 */
export class ActorQuerySourceDereferenceLinkHypermediaWrapCacheQuerySource extends ActorQuerySourceDereferenceLink {
  public readonly mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;
  public readonly mediatorMetadata: MediatorRdfMetadata;
  public readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;

  public readonly DF: DataFactory = new DataFactory();
  public readonly BF: BindingsFactory = new BindingsFactory(this.DF, {});
  public readonly AF: Factory = new Factory(this.DF);

  public constructor(args: IActorQuerySourceDereferenceLinkHypermediaWrapCacheQuerySourceArgs) {
    super(args);
    this.mediatorQuerySourceDereferenceLink = args.mediatorQuerySourceDereferenceLink;
    this.mediatorMetadata = args.mediatorMetadata;
    this.mediatorMetadataExtract = args.mediatorMetadataExtract;
  }

  public async test(action: IActionQuerySourceDereferenceLink): Promise<TestResult<IActorTest>> {
    if (action.context.get(KEY_WRAPPED)) {
      return failTest('Can only wrap dereference link once');
    }
    if (!action.context.get(KeysCaching.cacheManager)) {
      return failTest('Can only wrap dereference link with cache when manager is in context');
    }
    return passTestVoid();
  }

  public async run(action: IActionQuerySourceDereferenceLink): Promise<IActorQuerySourceDereferenceLinkOutput> {
    const context = action.link.context ? action.context.merge(action.link.context) : action.context;
    const cacheManager = context.getSafe(KeysCaching.cacheManager);

    let sourceFromCache: ISourceState | undefined;
    try {
      sourceFromCache = <ISourceState> await cacheManager.getFromCache(
        CacheEntrySourceState.cacheSourceStateQuerySource,
        CacheSourceStateViews.cacheQueryView,
        { url: action.link.url, mode: 'get', action },
      );
    } catch (err: any) {
      action.context.get(KeysCore.log)?.error(`Error when getting from cache: ${err.message}`);
      throw err;
    }
    if (sourceFromCache) {      
      // Re-extract traverse metadata so the followed links are up-to-date with current
      // query
      await sourceFromCache.source.getSelectorShape(new ActionContext());
      const traverse = await this.reExtractTraverseMetadata(sourceFromCache, action.link.url, context);
      sourceFromCache.metadata.traverse = traverse;
      // If we used cached source the cache will serve any matching bindings of the triple pattern,
      // so we return empty QuerySource for the aggregated store to import
      return { ...sourceFromCache, source: new QuerySourceStub(this.DF, action.link.url)};
    }
    action.context = action.context.set(KEY_WRAPPED, true);
    const dereferenceLinkOutput = await this.mediatorQuerySourceDereferenceLink.mediate(action);
    
    dereferenceLinkOutput.source = new QuerySourceCacheWrapper(dereferenceLinkOutput.source);
    
    await cacheManager.setCache(
      CacheEntrySourceState.cacheSourceStateQuerySource,
      action.link.url,
      { link: action.link, handledDatasets: action.handledDatasets!, ...dereferenceLinkOutput },
      { headers: dereferenceLinkOutput.headers },
    );
    return dereferenceLinkOutput;
  }

  protected async reExtractTraverseMetadata(source: ISourceState, url: string, context: IActionContext) {
    const quads = <RDF.Stream> source.source.queryQuads(
      this.AF.createPattern(
        this.DF.variable('s'),
        this.DF.variable('p'),
        this.DF.variable('o'),
        this.DF.variable('g'),
      ),
      context.set(KeysQueryOperation.unionDefaultGraph, true),
    );
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

    return metadataReExtract.traverse;
  }
}



export interface IActorQuerySourceDereferenceLinkHypermediaWrapCacheQuerySourceArgs extends IActorQuerySourceDereferenceLinkArgs {
  mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;
  mediatorMetadata: MediatorRdfMetadata;
  mediatorMetadataExtract: MediatorRdfMetadataExtract;
}

export const KEY_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/query-source-dereference-link-hypermedia-wrap-cache:wrapped',
);