import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';
import type { MediatorFactoryAggregatedStore } from '@comunica/bus-factory-aggregated-store';
import type {
  IActionOptimizeQueryOperation,
  IActorOptimizeQueryOperationArgs,
  IActorOptimizeQueryOperationOutput,
} from '@comunica/bus-optimize-query-operation';
import {
  ActorOptimizeQueryOperation,
} from '@comunica/bus-optimize-query-operation';
import type { IActionQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { CacheEntrySourceState, CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching, KeysInitQuery, KeysQueryOperation, KeysQuerySourceIdentify } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContext, ActionContextKey, passTestVoid } from '@comunica/core';
import type { BindingsStream, IActionContext, ILink, IQuerySource, ISourceState, ICacheView, IPersistentCache, ISetFn } from '@comunica/types';

import type { IAggregatedStore } from '@comunica/types-link-traversal';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import { AsyncIterator, UnionIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { PersistentCacheSourceStateIndexed } from './PersistentCacheSourceStateIndexed';
import { visitOperation } from '@comunica/utils-algebra/lib/utils';
import { ActorExtractLinksQuadPatternQuery } from '@comunica/actor-extract-links-quad-pattern-query';
import { QuerySourceCacheWrapper } from '@comunica/actor-context-preprocess-set-cache-source-state';
import { QuerySourceFileLazy } from '../../actor-query-source-identify-hypermedia-none-lazy/lib/QuerySourceFileLazy';
import { QuerySourceStub } from '../../actor-query-source-dereference-link-hypermedia-wrap-cache-query-source/lib/QuerySourceStub';

/**
 * A comunica Set Cache Query Source Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationSetCacheQuerySource extends ActorOptimizeQueryOperation {
  private cacheQuerySourceState: PersistentCacheSourceStateIndexed;
  private readonly cacheSizeNumTriples: number;

  public readonly mediatorFactoryAggregatedStore: MediatorFactoryAggregatedStore;
  public readonly actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;

  public constructor(args: IActorOptimizeQueryOperationSetCacheQuerySourceArgs) {
    super(args);
    this.cacheSizeNumTriples = args.cacheSizeNumTriples;
    this.mediatorFactoryAggregatedStore = args.mediatorFactoryAggregatedStore;
    this.actorExtractLinksQuadPatternQuery = args.actorExtractLinksQuadPatternQuery;

    this.cacheQuerySourceState = new PersistentCacheSourceStateIndexed(
      { maxNumTriples: args.cacheSizeNumTriples },
    );
    console.log(`Created cache with maxSize: ${args.cacheSizeNumTriples}`)
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    const context = action.context;
    if (!action.context.get(KeysQuerySourceIdentify.traverse)) {
      return { context, operation: action.operation };
    }

    if (context.get(KeysCaching.clearCache) || context.get(new ActionContextKey('clearCache'))) {
      this.cacheQuerySourceState = new PersistentCacheSourceStateIndexed(
        { maxNumTriples: this.cacheSizeNumTriples },
      );
      console.log(`Cleaned cache, size: ${await this.cacheQuerySourceState.size()}`);
    }

    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceStateQuerySource,
      this.cacheQuerySourceState,
      new SetSourceStateCache(),
    );
    const quadPatterns = this.extractQuadPatterns(action.context.getSafe(KeysInitQuery.query));
    console.log(ActorExtractLinksQuadPatternQuery.getCurrentQuery(action.context)!);
    cacheManager.registerCacheView(
      CacheSourceStateViews.cacheQueryView,
      new GetStreamingCacheView(
        quadPatterns,
        ActorExtractLinksQuadPatternQuery.getCurrentQuery(action.context)!,
        (await this.mediatorFactoryAggregatedStore.mediate({ context })).aggregatedStore,
        this.actorExtractLinksQuadPatternQuery,
        context.get(KeysQueryOperation.unionDefaultGraph),
      ),
    );

    return { context, operation: action.operation };
  }

  /**
   * Extracts all quad patterns from a given SPARQL Algebra AST.
   * @param ast The root operation node of the query.
   * @returns An array of all pattern nodes found in the AST.
   */
  private extractQuadPatterns(ast: Algebra.BaseOperation): Algebra.Pattern[] {
    const quadPatterns: Algebra.Pattern[] = [];

    visitOperation(ast, {
      [Algebra.Types.PATTERN]: {
        visitor: (node: Algebra.Pattern) => {
          quadPatterns.push(node);
        },
      },
    });

    return quadPatterns;
  }
}

export class SetSourceStateCache implements ISetFn<ISourceState, ISourceState, { headers: Headers }> {
  protected DF: DataFactory = new DataFactory();
  protected AF: AlgebraFactory = new AlgebraFactory(this.DF);

  public async setInCache(
    key: string,
    value: ISourceState,
    cache: IPersistentCache<ISourceState>,
    context: { headers: Headers },
  ): Promise<void> {
    cache.set(key, value);
  }
}

export class GetStreamingCacheView implements ICacheView<
  ISourceState,
  { url: string; mode: 'get'; action: IActionQuerySourceDereferenceLink } | { mode: 'queryBindings' | 'queryQuads'; operation: Algebra.Operation; context: IActionContext },
  BindingsStream | AsyncIterator<RDF.Quad> | ISourceState
> {
  protected traverseEnded = false;
  protected pendingCount = 0;

  protected readonly DF: DataFactory = new DataFactory();
  protected readonly BF: BindingsFactory = new BindingsFactory(this.DF);
  protected readonly AF: AlgebraFactory = new AlgebraFactory(this.DF);

  protected readonly cachedStore: IAggregatedStore;
  protected readonly accumulatedSource: IQuerySource;

  protected readonly quadPatterns: Algebra.Pattern[];
  protected readonly queryOperation: Algebra.Operation;
  protected readonly unionDefaultGraph: boolean;
  protected allIteratorsClosedListener: () => void;

  protected readonly actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;

  importedQuadsToCache = 0;

  public constructor(
    topLevelQuadPatterns: Algebra.Pattern[],
    queryOperation: Algebra.Operation,
    aggregatedStore: IAggregatedStore,
    actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery,
    unionDefaultGraph?: boolean,
  ) {
    this.cachedStore = aggregatedStore;

    this.quadPatterns = topLevelQuadPatterns;
    this.queryOperation = queryOperation;
    this.unionDefaultGraph = Boolean(unionDefaultGraph);

    this.allIteratorsClosedListener = () => this.checkForTermination();
    this.cachedStore.addAllIteratorsClosedListener(this.allIteratorsClosedListener);

    this.accumulatedSource = new QuerySourceRdfJs(
      <RDF.Source | RDF.DatasetCore> this.cachedStore,
      this.DF,
      this.BF,
    );

    this.actorExtractLinksQuadPatternQuery = actorExtractLinksQuadPatternQuery;
  }

  protected checkForTermination(cache?: IPersistentCache<ISourceState>) {
    // If the query execution is marked as ended (when traversal ends for example)
    // and we finished importing all streams, we end the cached store.
    if (this.traverseEnded && this.pendingCount === 0 && !this.cachedStore.hasEnded()) {
      this.cachedStore.end();
      if (cache) {
        console.log(cache.endSession());
      }
      this.cachedStore.removeAllIteratorsClosedListener(this.allIteratorsClosedListener);
    }
  }

  public async construct(
    cache: IPersistentCache<ISourceState>,
    context: { url: string; mode: 'get'; action: IActionQuerySourceDereferenceLink; extractLinksQuadPattern?: boolean } | { mode: 'queryBindings' | 'queryQuads'; operation: Algebra.Operation; context: IActionContext },
  ): Promise<BindingsStream | AsyncIterator<RDF.Quad> | ISourceState | undefined> {
    if (context.mode === 'get') {
      // When passed end event, traversal is done and the source can end if we finished
      // importing all data from previous 'get' requests to cache
      if (context.url === 'end') {
        this.traverseEnded = true;
        this.checkForTermination(cache);
        return;
      }

      const cacheEntry = await cache.get(context.url);
      // Only push if valid and policy satisfied
      if (cacheEntry && cacheEntry.cachePolicy?.satisfiesWithoutRevalidation(context.action)) {
        this.pendingCount += this.quadPatterns.length;
        
        if (context.extractLinksQuadPattern && this.actorExtractLinksQuadPatternQuery) {
          const links: ILink[] = [];
          // const quads = new UnionIterator(this.quadPatterns.map(
          //   (quadPattern) => cacheEntry.source.queryQuads(quadPattern, 
          //     new ActionContext().set(KeysQueryOperation.unionDefaultGraph, true))
          // ), { "autoStart": false });
          // const doneQuads = new Promise((resolve) => {
          //   quads.on('data', () => {
          //     this.quadsFromCache++;
          //   })
          //   quads.on('end', resolve)
          // })
          // await doneQuads;
          // for (const quadPattern of this.quadPatterns) {
           // Create a temporary fast-stream strictly for extracting links from the cache
          // const quads = <RDF.Stream> cacheEntry.source.queryQuads(
          //   this.AF.createPattern(
          //     this.DF.variable('s'),
          //     this.DF.variable('p'),
          //     this.DF.variable('o'),
          //     this.DF.variable('g'),
          //   ),
          //   new ActionContext().set(KeysQueryOperation.unionDefaultGraph, true),
          // );
          
          // // const extractionStream = cacheEntry.source.queryQuads(this.quadPatterns[0], context.action.context);
          // const patternLinks = await ActorExtractLinksQuadPatternQuery
          //   .collectStream(quads, (quad, arr) => {
          //     ActorExtractLinksQuadPatternQuery.extractLinksOnQuad(
          //       quad,
          //       arr,
          //       this.queryOperation,
          //       true,
          //       this.constructor.name,
          //     );
          // });
          // links.push(...patternLinks);
          // // }

          // // Update the query-dependent part of the traverse metadata
          // const staticTraverseEntries = cacheEntry.metadata.traverse.filter(
          //   (x: ILink) => x.metadata?.producedByActor.name !== this.actorExtractLinksQuadPatternQuery!.name
          // );
          // cacheEntry.metadata.traverse = [...staticTraverseEntries, ...links];
        }
        const matchingQuads = await Promise.all(this.quadPatterns.map((quadPattern) => {
          return cacheEntry.source.queryQuads(quadPattern, context.action.context)
        }));
        const source = new QuerySourceFileLazy(
          new UnionIterator(matchingQuads, {"autoStart": false}),
          this.DF,
          context.url,
          async quads => new QuerySourceStub(this.DF, context.url) ,
        );
        
        return {...cacheEntry, source}
      }
      return;
        // const importDone = Promise.all(this.quadPatterns.map((quadPattern) => {
        //   return new Promise<void>((resolve, reject) => {
        //     const importStream = this.cachedStore.import(
        //       cacheEntry.source.queryQuads(quadPattern, context.action.context),
        //     );

        //     // We no longer extract links here. We only manage termination.
        //     importStream.on('end', () => {
        //       this.pendingCount--;
        //       this.checkForTermination(cache);
        //       resolve()
        //     });

        //     importStream.on('error', (err) => {
        //       console.error('Import stream error:', err);
        //       this.pendingCount--;
        //       this.checkForTermination(cache);
        //       reject()
        //     });
        //   })
        // }));
        // await importDone;
        // // Map streams to promises to await their completion
        // const importPromises = this.quadPatterns.map((quadPattern) => {
        //   return new Promise<void>((resolve) => {
        //     // Directly import only matching quads from source to the cache-based streamingStore.
        //     const importStream = this.cachedStore.import(
        //       cacheEntry.source.queryQuads(quadPattern, context.action.context),
        //     );

        //     if (context.extractLinksQuadPattern && this.actorExtractLinksQuadPatternQuery) {
        //       // Re-extract cMatch criterion on the imported quad stream.
        //       importStream.on('data', (quad) => {
        //         ActorExtractLinksQuadPatternQuery.extractLinksOnQuad(
        //           quad,
        //           links,
        //           quadPattern,
        //           true,
        //           this.constructor.name,
        //         );
        //       });
        //     }

        //     importStream.on('end', () => {
        //       this.pendingCount--;
        //       resolve();
        //     });

        //     importStream.on('error', (err) => {
        //       console.error('Import stream error:', err);
        //       this.pendingCount--;
        //       resolve();
        //     });
        //   });
        // });
        // const currentTraverse = Promise.resolve(cacheEntry.metadata.traverse);

        // cacheEntry.metadata.traverse = Promise.all(importPromises).then(async () => {
        //   const resolvedTraverse = await currentTraverse;
          
        //   let updatedTraverse = resolvedTraverse;
        //   if (context.extractLinksQuadPattern && this.actorExtractLinksQuadPatternQuery) {
        //     const staticTraverseEntries = resolvedTraverse.filter(
        //       (x: ILink) => x.metadata?.producedByActor.name !== this.actorExtractLinksQuadPatternQuery!.name
        //     );
        //     updatedTraverse = [...staticTraverseEntries, ...links];
        //   }
          
        //   this.checkForTermination(cache);
        //   return updatedTraverse;
        // });
        // // Attach a Promise to the metadata for downstream synchronization
        // cacheEntry.metadata.traverse = Promise.all(importPromises).then(() => {
        //   if (context.extractLinksQuadPattern && this.actorExtractLinksQuadPatternQuery) {
        //     const staticTraverseEntries = cacheEntry.metadata.traverse.filter(
        //       (x: ILink) => x.metadata?.producedByActor.name !== this.actorExtractLinksQuadPatternQuery!.name
        //     );
        //     cacheEntry.metadata.traverse = [...staticTraverseEntries, ...links];
        //   }
          
        //   this.checkForTermination(cache);
        //   return cacheEntry.metadata.traverse;
        // });
        // Promise.all(importPromises).then(() => {
        //   // Update traverse metadata based on found links
        //   if (context.extractLinksQuadPattern && this.actorExtractLinksQuadPatternQuery) {
        //     const staticTraverseEntries = cacheEntry.metadata.traverse.filter(
        //       (x: ILink) => x.metadata?.producedByActor.name !== this.actorExtractLinksQuadPatternQuery!.name
        //     );
        //     const newTraverse = [...staticTraverseEntries, ...links];
        //     cacheEntry.metadata.traverse = newTraverse;
        //   }
          
        //   this.checkForTermination(cache);
        // });
      //   return cacheEntry;
      // }
      // if (cacheEntry && cacheEntry.cachePolicy?.satisfiesWithoutRevalidation(context.action)) {
      //   this.pendingCount += this.quadPatterns.length;
        
      //   const links: ILink[] = [];
      //   for (const quadPattern of this.quadPatterns) {
      //     // Directly import only matching quads from source to the store.
      //     const importStream = this.cachedStore.import(
      //       cacheEntry.source.queryQuads(quadPattern, context.action.context),
      //     );
      //     if (context.extractLinksQuadPattern && this.actorExtractLinksQuadPatternQuery) {
      //       // Re-extract cMatch criterion on the imported quad stream.
      //       // importStream.on('data', (quad) => {
      //       //   ActorExtractLinksQuadPatternQuery.extractLinksOnQuad(
      //       //     quad,
      //       //     links,
      //       //     quadPattern,
      //       //     true,
      //       //     this.constructor.name,
      //       //   );
      //       // });
      //     }

      //     importStream.on('end', () => {
      //       this.pendingCount--;
      //       // if (context.extractLinksQuadPattern && this.actorExtractLinksQuadPatternQuery) {
      //       //   // TODO: This should only be run when all quad patterns have finished importing and should be awaited
      //       //   // Please fix
      //       //   const newTraverse = cacheEntry.metadata.traverse.filter(
      //       //     (x: ILink) => x.metadata?.producedByActor.name !== this.actorExtractLinksQuadPatternQuery!.name
      //       //   );
      //       //   console.log("Mapped")
      //       //   console.log(cacheEntry.metadata.traverse.map((x: ILink) => { return {produced: x.metadata?.producedByActor, url: x.url}}));
      //       //   console.log("Filtered")
      //       //   console.log(newTraverse);
      //       //   console.log("Re-extracted links")
      //       //   console.log(links);
      //       //   console.log(this.actorExtractLinksQuadPatternQuery.name);
      //       // }
      //       this.checkForTermination(cache);
      //     });

      //     importStream.on('error', (err) => {
      //       console.error('Import stream error:', err);
      //       this.pendingCount--;
      //       this.checkForTermination(cache);
      //     });
      //   }

      //   return cacheEntry;
      // }
      // return;
    }
    if (context.mode === 'queryBindings') {
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        return this.accumulatedSource.queryBindings(context.operation, context.context);
      }
      throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
    } else if (context.mode === 'queryQuads') {
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        return this.accumulatedSource.queryQuads(context.operation, context.context);
      }
      throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
    }
    throw new Error(`Unknown view mode passed to ${this.constructor.name}: ${context.mode}`);
  }
}

export interface IActorOptimizeQueryOperationSetCacheQuerySourceArgs extends IActorOptimizeQueryOperationArgs {
  /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124000}
   */
  cacheSizeNumTriples: number;
  /**
   * A mediator for creating aggregated stores
   */
  mediatorFactoryAggregatedStore: MediatorFactoryAggregatedStore;
  /**
   * Optional actor to execute cMatch traversal criterion on cached sources.
   * This should always be passed when cMatch is used, as cached sources contain stale
   * traversal metadata entries otherwise.
   */
  actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;
}
