import type {
  IActionContextPreprocess,
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { CacheEntrySourceState } from '@comunica/cache-manager-entries/lib';
import { CacheSourceStateView } from '@comunica/cache-manager-entries/lib/ViewKeys';
import { KeysCaching, KeysQuerySourceIdentifyHypermediaNoneLazy } from '@comunica/context-entries-link-traversal';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { ActionContext, passTestVoid } from '@comunica/core';
import type { ISourceState } from '@comunica/types';

import { ICacheView, IPersistentCache, ISetFn } from '@comunica/types-link-traversal';
import { PersistentCacheSourceStateNumTriples } from './PersistentCacheSourceStateNumTriples';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { RdfStore } from 'rdf-stores';
import type * as RDF from '@rdfjs/types';
import { IActorRdfMetadataOutput } from '@comunica/bus-rdf-metadata';
import { QuerySourceCacheWrapper } from './QuerySourceCacheWrapper';

/**
 * A comunica Set Defaults Traversal Caching Context Preprocess Actor.
 */
export class ActorContextPreprocessSetDefaultsTraversalCachingNumTriples extends ActorContextPreprocess {
  private readonly cacheSourceState: PersistentCacheSourceStateNumTriples;

  public constructor(args: IActorContextPreprocessSetSourceCacheNumTriplesArgs) {
    super(args);
    this.cacheSourceState = new PersistentCacheSourceStateNumTriples(
      { maxNumTriples: args.cacheSizeNumTriples }
    )
    // this.cacheSourceState = new LRUCache<string, ISourceState>({
    //   maxSize: args.cacheSizeNumTriples,
    //   sizeCalculation: ActorContextPreprocessSetDefaultsTraversalCachingNumTriples.getSizeSource,
    // });
  }

  public async test(_action: IAction): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    const context = action.context;
    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceState,
      this.cacheSourceState,
      new SetSourceStateCache(),
    );
    cacheManager.registerCacheView(
      CacheSourceStateView.cacheSourceStateView,
      new GetSourceStateCacheView()
    )
    return { context };
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
    const cacheSource = new QuerySourceCacheWrapper(value.source);
    await cacheSource.ingestQuads();
    const cachableSourceState = {...value, source: cacheSource };
    cache.set(key, cachableSourceState);
    // const store = RdfStore.createDefault();
    // const quads = value.source.queryQuads(
    //   this.AF.createPattern(
    //       this.DF.variable('s'),
    //       this.DF.variable('p'),
    //       this.DF.variable('o'),
    //       this.DF.variable('g')
    //   ),
    //   new ActionContext({ 
    //     [KeysQuerySourceIdentifyHypermediaNoneLazy.nonConsumingQueryQuads.name]: true
    //   })
    // );
    // const promiseConsumedSource = new Promise<void>((resolve, reject) => {
    //   quads.on('data', (quad) => store.addQuad(quad));
    //   quads.on('end', () => {
    //     cache.set(key, { store, headers: context["headers"] });
    //     resolve();
    //   });
    //   quads.on('error', () => reject("Error importing quads for cached source"));
    // })
    // return promiseConsumedSource;
  }
}

export class GetSourceStateCacheView 
implements ICacheView<ISourceState, { url: string }, ISourceState> {
  public async construct(cache: IPersistentCache<ISourceState>, context: { url: string }): Promise<ISourceState | undefined> {
    const cacheEntry = await cache.get(context.url);
    if (!cacheEntry){
      return;
    }
    return cacheEntry;
  }
}

export interface IActorContextPreprocessSetSourceCacheNumTriplesArgs extends IActorContextPreprocessArgs {
  /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124_000}
   */
  cacheSizeNumTriples: number;
}
