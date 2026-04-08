import type {
  IActionContextPreprocess,
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { CacheEntrySourceState } from '@comunica/cache-manager-entries/lib';
import { CacheSourceStateViews } from '@comunica/cache-manager-entries/lib/ViewKeys';
import { KeysCaching, KeysInitQuery, KeysQueryOperation } from '@comunica/context-entries';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { ActionContext, ActionContextKey, passTestVoid } from '@comunica/core';
import type { ISourceState, ICacheView, IPersistentCache, ISetFn, ILink, ComunicaDataFactory, IActionContext } from '@comunica/types';

import { AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { PersistentCacheSourceStateNumTriples } from './PersistentCacheSourceStateNumTriples';
import { ActorExtractLinksQuadPatternQuery } from '@comunica/actor-extract-links-quad-pattern-query';
import { IActionQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { MediatorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';

/**
 * A comunica Set Defaults Traversal Caching Context Preprocess Actor.
 */
export class ActorContextPreprocessSetCacheSourceState extends ActorContextPreprocess {
  protected readonly actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;
  protected readonly mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;

  private readonly cacheSizeNumTriples: number;
  private cacheSourceState: PersistentCacheSourceStateNumTriples;
  private cacheDeserializationDone: Promise<void>;

  public readonly probabilityCacheMiss?: number;

  public constructor(args: IActorContextPreprocessSetSourceCacheNumTriplesArgs) {
    super(args);
    this.cacheSizeNumTriples = args.cacheSizeNumTriples;
    this.mediatorQuerySourceIdentifyHypermedia = args.mediatorQuerySourceIdentifyHypermedia;
    this.cacheSourceState = new PersistentCacheSourceStateNumTriples(
      { 
        maxNumTriples: args.cacheSizeNumTriples, 
        mediatorQuerySourceIdentifyHypermedia: this.mediatorQuerySourceIdentifyHypermedia,
        serializationLoc: "temp-cache-content.json" 
      },
    );
    this.cacheDeserializationDone = this.cacheSourceState.deserialize();
    this.actorExtractLinksQuadPatternQuery = args.actorExtractLinksQuadPatternQuery;
    this.probabilityCacheMiss = args.probabilityCacheMiss;

    console.log(`Created unindexed cache with maxSize: ${args.cacheSizeNumTriples}`)
  }

  public async test(_action: IAction): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    await this.cacheDeserializationDone;
    const context = action.context;
    const cacheManager = context.getSafe(KeysCaching.cacheManager);

    // TEMP Solution due to my own sparql benchmark runner adjustments
    if (context.get(KeysCaching.clearCache) || context.get(new ActionContextKey('clearCache'))) {
      this.cacheSourceState = new PersistentCacheSourceStateNumTriples(
        { 
          maxNumTriples: this.cacheSizeNumTriples, 
          mediatorQuerySourceIdentifyHypermedia: this.mediatorQuerySourceIdentifyHypermedia,
          serializationLoc: "temp-cache-content.json"
        },
      );
      console.log(`Cleaned cache, size: ${await this.cacheSourceState.size()}`);
    }

    const timeoutCallbacks = context.get(KeysInitQuery.timeoutCallbacks);
    if (timeoutCallbacks){
      console.log("Adding serialization callback to timeout callbacks");
      timeoutCallbacks.push(async () => await this.cacheSourceState.serialize());
    }

    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceState,
      this.cacheSourceState,
      new SetSourceStateCache(),
    );

    const debugLogger = this.logDebug.bind(this);

    cacheManager.registerCacheView(
      CacheSourceStateViews.cacheSourceStateView,
      new GetSourceStateCacheView(
        new DataFactory(),
        debugLogger,
        this.actorExtractLinksQuadPatternQuery,
        this.probabilityCacheMiss,
      ),
    );
    return { context };
  }
}

export class SetSourceStateCache implements ISetFn<ISourceState, ISourceState, { headers: Headers }> {
  public async setInCache(
    key: string,
    value: ISourceState,
    cache: IPersistentCache<ISourceState>,
    context: { headers: Headers },
  ): Promise<void> {
    cache.set(key, value);
  }
}

export class GetSourceStateCacheView
implements ICacheView<
    ISourceState, 
    {       
      url: string,
      extractLinksQuadPattern?: boolean,
      action: IActionQuerySourceDereferenceLink;
    }, 
    ISourceState
  > {
  protected debugLogger: 
    (context: IActionContext, message: string, data?: (() => any)) => void;
  protected readonly dataFactory: ComunicaDataFactory;
  protected readonly algebraFactory: AlgebraFactory;
  protected readonly actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;
  protected readonly probabilityCacheMiss?: number;

  protected simulatedMisses: number = 0;
  protected hits: number = 0;

  public constructor(
    dataFactory: ComunicaDataFactory,
    debugLogger: (context: IActionContext, message: string, data?: (() => any)) => void,
    actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery,
    probabilityCacheMiss?: number,
  ){
    this.dataFactory = dataFactory;
    this.algebraFactory = new AlgebraFactory(this.dataFactory);

    this.debugLogger = debugLogger;

    this.actorExtractLinksQuadPatternQuery = actorExtractLinksQuadPatternQuery;
    this.probabilityCacheMiss = probabilityCacheMiss;
  }

  public async construct(
    cache: IPersistentCache<ISourceState>, 
    context: { 
      url: string,
      extractLinksQuadPattern?: boolean,
      action: IActionQuerySourceDereferenceLink;
    }
  ): Promise<ISourceState | undefined> {
    const cacheEntry = await cache.get(context.url);
    if (!cacheEntry) {
      return;
    }
    this.hits++
    if (this.probabilityCacheMiss){
      if (Math.random() < this.probabilityCacheMiss){
        this.simulatedMisses++
        return;
      }
    }
    console.log(`Simulated miss, rate: ${this.simulatedMisses/this.hits}`);

    if (context.extractLinksQuadPattern && this.actorExtractLinksQuadPatternQuery){
      const queryOp = context.action.context.getSafe(KeysInitQuery.query);
      const links: ILink[] = [];
      const quads = cacheEntry.source.queryQuads(
        this.algebraFactory.createPattern(
          this.dataFactory.variable('s'),
          this.dataFactory.variable('p'),
          this.dataFactory.variable('o'),
          this.dataFactory.variable('g'),
        ),
        context.action.context,
      );

      const patternLinks = await ActorExtractLinksQuadPatternQuery
        .collectStream(quads, (quad, arr) => {
          ActorExtractLinksQuadPatternQuery.extractLinksOnQuad(
            quad,
            arr,
            queryOp,
            true,
            this.actorExtractLinksQuadPatternQuery!.name,
          );
      });

      links.push(...patternLinks);
      const staticTraverseEntries = cacheEntry.metadata.traverse.filter(
        (x: ILink) => x.metadata?.producedByActor.name !== this.actorExtractLinksQuadPatternQuery!.name
      );
      cacheEntry.metadata.traverse = [...staticTraverseEntries, ...links];
    }
    return cacheEntry;
  }
}

export interface IActorContextPreprocessSetSourceCacheNumTriplesArgs extends IActorContextPreprocessArgs {
  /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124000}
   */
  cacheSizeNumTriples: number;
  /**
   * Mediator used to rehydrate cache entries
   */
  mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;
  /**
   * Optional actor to execute cMatch traversal criterion on cached sources.
   * This should always be passed when cMatch is used, as cached sources contain stale
   * traversal metadata entries otherwise.
   */
  actorExtractLinksQuadPatternQuery?: ActorExtractLinksQuadPatternQuery;  /**
  /**
   * For simulating cache misses
   * @range {float}
   */
  probabilityCacheMiss?: number;
}
