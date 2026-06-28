import { QuerySourceCacheWrapper } from '@comunica/caches-link-traversal';
import {
  ActorQuerySourceDereferenceLink,
} from '@comunica/bus-query-source-dereference-link';
import type {
  IActionQuerySourceDereferenceLink,
  IActorQuerySourceDereferenceLinkOutput,
  IActorQuerySourceDereferenceLinkArgs,
  MediatorQuerySourceDereferenceLink,
} from '@comunica/bus-query-source-dereference-link';
import type { MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import type { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import type { ICacheKey, IViewKey } from '@comunica/cache-manager-entries';
import { CacheKey, ViewKey } from '@comunica/cache-manager-entries';
import { KeysCore, KeysStatistics, KeysCaching } from '@comunica/context-entries';
import type { TestResult, IActorTest } from '@comunica/core';
import { ActionContextKey, failTest, passTestVoid } from '@comunica/core';
import type { ISourceState } from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';

import { DataFactory } from 'rdf-data-factory';
import { Factory } from 'sparqlalgebrajs';

/**
 * A comunica Dereference Query Source Hypermedia Resolve Actor.
 */
export class ActorQuerySourceDereferenceLinkHypermediaWrapCache extends ActorQuerySourceDereferenceLink {
  public readonly mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;
  public readonly mediatorMetadata: MediatorRdfMetadata;
  public readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;

  public readonly cacheEntryKey: ICacheKey<unknown, unknown, unknown>;
  public readonly cacheViewKey: IViewKey<unknown, unknown, unknown>;
  public readonly cacheSetKeys?: ICacheKey<unknown, unknown, unknown>[];

  public readonly DF: DataFactory = new DataFactory();
  public readonly BF: BindingsFactory = new BindingsFactory(this.DF, {});
  public readonly AF: Factory = new Factory(this.DF);

  public constructor(args: IActorQuerySourceDereferenceLinkHypermediaWrapCacheArgs) {
    super(args);
    this.mediatorQuerySourceDereferenceLink = args.mediatorQuerySourceDereferenceLink;
    this.mediatorMetadata = args.mediatorMetadata;
    this.mediatorMetadataExtract = args.mediatorMetadataExtract;
    this.cacheEntryKey = new CacheKey(args.cacheEntryKeyName);
    this.cacheViewKey = new ViewKey(args.cacheViewKeyName);
    this.cacheSetKeys = args.cacheSetKeyNames?.map(
      cacheSetKeyName => new CacheKey(cacheSetKeyName)
    );
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
      sourceFromCache = <ISourceState | undefined> await cacheManager.getFromCache(
        this.cacheEntryKey,
        this.cacheViewKey,
        { url: action.link.url, action, extractLinksQuadPattern: true },
      );
    } catch (err: any) {
      action.context.get(KeysCore.log)?.error(`Error when getting from cache: ${err.message}`);
      throw err;
    }
    if (sourceFromCache && await sourceFromCache.cachePolicy?.satisfiesWithoutRevalidation(action)) {
      context.get(KeysStatistics.dereferencedLinks)?.updateStatistic(
        {
          url: action.link.url,
          metadata: { ...sourceFromCache.metadata, cached: true },
        },
        sourceFromCache,
      );
      return sourceFromCache;
    }
    action.context = action.context.set(KEY_WRAPPED, true);
    const dereferenceLinkOutput = await this.mediatorQuerySourceDereferenceLink.mediate(action);

    dereferenceLinkOutput.source = new QuerySourceCacheWrapper(dereferenceLinkOutput.source);
    await cacheManager.setCache(
      this.cacheEntryKey,
      action.link.url,
      { link: action.link, handledDatasets: action.handledDatasets!, ...dereferenceLinkOutput },
      { headers: dereferenceLinkOutput.headers },
    );
    
    // Optionally set additional cache entries.
    if (this.cacheSetKeys){
      for (const key of this.cacheSetKeys){
        await cacheManager.setCache(
          key,
          action.link.url,
          { link: action.link, handledDatasets: action.handledDatasets!, ...dereferenceLinkOutput },
          { headers: dereferenceLinkOutput.headers },
        );
      }
    }
    return dereferenceLinkOutput;
  }
}

export interface IActorQuerySourceDereferenceLinkHypermediaWrapCacheArgs extends IActorQuerySourceDereferenceLinkArgs {
  mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;
  mediatorMetadata: MediatorRdfMetadata;
  mediatorMetadataExtract: MediatorRdfMetadataExtract;
  cacheEntryKeyName: string;
  cacheViewKeyName: string;
  /**
   * Optional additional keys to set into cache. These won't be used here
   * but can set values used in other parts of the query engine.
   */
  cacheSetKeyNames?: string[];
}

export const KEY_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/query-source-dereference-link-hypermedia-wrap-cache:wrapped',
);
