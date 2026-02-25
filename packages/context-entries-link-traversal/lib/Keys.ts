import { ActionContextKey } from '@comunica/core';
import type {
  AnnotateSourcesType,
  LinkFilter,
  IAggregatedStore,
  ILinkTraversalManager,
} from '@comunica/types-link-traversal';
import { IDerivedResourceUnidentified } from '../../actor-extract-links-solid-derived-resources/lib';
import { IDerivedResourcesContainer } from '../../actor-context-preprocess-set-defaults-link-traversal/lib';

/**
 * When adding entries to this file, also add a shortcut for them in the contextKeyShortcuts TSDoc comment in
 * ActorIniQueryBase in @comunica/actor-init-query if it makes sense to use this entry externally.
 * Also, add this shortcut to IQueryContextCommon in @comunica/types.
 */

export const KeysRdfResolveHypermediaLinks = {
  /**
   * Context entry for indicating the type of source annotation.
   */
  annotateSources: new ActionContextKey<AnnotateSourcesType>(
    '@comunica/bus-rdf-resolve-hypermedia-links:annotateSources',
  ),
  /**
   * Context entry containing the link filters applied on link queues within the context scope.
   * Setting this entry too high in the context hierarchy could result in too much being filtered out.
   */
  linkFilters: new ActionContextKey<LinkFilter[]>(
    '@comunica/bus-rdf-resolve-hypermedia-links:linkFilters',
  ),

  dynamicFilter: new ActionContextKey<Set<string>>(
    '@comunica/bus-rdf-resolve-hypermedia-links-queue:filterDynamic'
  ),

};

export const KeysExtractLinksTree = {
  /**
   * A flag to indicate if relationships should strictly correspond to the current document's URL.
   * Default true.
   */
  strictTraversal:
 new ActionContextKey<boolean>('@comunica/actor-extract-links-tree:strictTraversal'),
};

export const KeysRdfJoin = {
  /**
   * If adaptive joining must not be done.
   */
  skipAdaptiveJoin: new ActionContextKey<boolean>('@comunica/bus-rdf-join:skipAdaptiveJoin'),
};

export const KeysQuerySourceIdentifyLinkTraversal = {
  /**
   * Aggregated store for a traversal link traversal source.
   */
  linkTraversalAggregatedStore: new ActionContextKey<IAggregatedStore>(
    '@comunica/bus-query-source-identify:linkTraversalAggregatedStore',
  ),
  /**
   * Manager for the link traversal.
   */
  linkTraversalManager: new ActionContextKey<ILinkTraversalManager>(
    '@comunica/bus-query-source-identify:linkTraversalManager',
  ),
};

export const KeysDerivedResourceIdentify = {
  derivedResourcesUnidentified: new ActionContextKey<IDerivedResourceUnidentified[]>(
    '@comunica/bus-derived-resource-identify:derivedResourcesUnidentified',
  ),
  //TODO: Rename this properly when things work
  derivedResourcesContainer: new ActionContextKey<IDerivedResourcesContainer>(
    '@comunica/bus-derived-resource-identify:derivedResourcesContainer',
  )
}