import { ActionContextKey } from '@comunica/core';
import type { AnnotateSourcesType } from '@comunica/types-link-traversal';
import { ITopologyUpdate } from '@comunica/statistic-traversal-topology';
import { IStatisticBase } from '@comunica/types';
import { ITopologyUpdateRccUpdate, TopologyUpdateRccEmit } from '@comunica/statistic-traversal-topology-rcc';

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

  writeToFile: new ActionContextKey<IStatisticBase<any>>('@comunica/statistic-write-to-file:Writer')
};
