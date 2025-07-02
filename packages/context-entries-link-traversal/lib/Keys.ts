import { ActionContextKey } from '@comunica/core';
import type { ITopologyUpdate } from '@comunica/statistic-traversal-topology';
import type { TopologyUpdateRccEmit } from '@comunica/statistic-traversal-topology-rcc';
import type { IStatisticBase } from '@comunica/types';
import type { AnnotateSourcesType, LinkFilter } from '@comunica/types-link-traversal';

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

export const KeysStatisticsTraversal = {
  traversalTopology: new ActionContextKey<IStatisticBase<ITopologyUpdate>>
  ('@comunica/statistic-traversal-topology:Tracked'),
  traversalTopologyRcc: new ActionContextKey<IStatisticBase<TopologyUpdateRccEmit>>
  ('@comunica/statistic-traversal-topology-rcc:Tracked'),
  writeToFile: new ActionContextKey<IStatisticBase<any>>('@comunica/statistic-write-to-file:Writer'),
  nestedQuery: new ActionContextKey<Boolean>('@comunica/statistic-base:nested')
};
