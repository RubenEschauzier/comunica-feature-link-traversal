import type {
  IActionContextPreprocess,
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { CacheEntrySourceState, CacheSourceStateViews } from '@comunica/cache-manager-entries';
import { KeysCaching } from '@comunica/context-entries-link-traversal';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContext, passTestVoid } from '@comunica/core';
import type { Bindings, BindingsStream, ISourceState } from '@comunica/types';

import type { ICacheView, IPersistentCache, ISetFn } from '@comunica/types-link-traversal';
import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { PersistentCacheSourceStateIndexed } from './PersistentCacheSourceStateIndexed';
import { BufferedIterator } from 'asynciterator';

/**
 * A comunica Set Cache Query Source Context Preprocess Actor.
 */
export class ActorContextPreprocessSetCacheQuerySource extends ActorContextPreprocess {
  private readonly cacheQuerySourceState: PersistentCacheSourceStateIndexed;

  public constructor(args: IActorContextPreprocessSetCacheQuerySourceArgs) {
    super(args);
    this.cacheQuerySourceState = new PersistentCacheSourceStateIndexed(
      { maxNumTriples: args.cacheSizeNumTriples },
    );

  }

  public async test(action: IActionContextPreprocess): Promise<TestResult<IActorTest>> {
    return passTestVoid(); // TODO implement
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    const context = action.context;
    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceState,
      this.cacheQuerySourceState,
      new SetSourceStateCache(),
    );

    cacheManager.registerCacheView(
      CacheSourceStateViews.cacheQueryView,
      new GetSourceStateCacheView(),
    );
    
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
    cache.set(key, value);
  }
}
export class GetSourceStateCacheView
implements ICacheView<
  ISourceState, 
  { url: string, mode: 'get' | 'query', operation: Algebra.Operation },
  BindingsStream | ISourceState
> {
  // List of active queries waiting for data
  private queryListeners: Array<(doc: ISourceState) => void> = [];

  public async construct(
    cache: IPersistentCache<ISourceState>,
    context: { url: string, mode: 'get' | 'query', operation: Algebra.Operation }
  ): Promise<ISourceState | BindingsStream | undefined> {
    if (context.mode === 'query'){
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        // Iterator we will push bindings to when this view is called with 'get'
        const result = new BufferedIterator<Bindings>({ autoStart: false });

        const onCacheGet = ( document: ISourceState ) => {
          const bindingStream = document.source.queryBindings(context.operation, new ActionContext());
          bindingStream.on('data', (data) => (<any>result)._push(data));
        }

        this.queryListeners.push(onCacheGet);

        // Cleanup when the query stream is destroyed
        const originalDestroy = result.destroy.bind(result);
        result.destroy = (cause) => {
          this.queryListeners = this.queryListeners.filter(l => l !== onCacheGet);
          return originalDestroy(cause);
        };
        return result;
      }
      else {
        throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
      }
    }
    else if (context.mode === 'get'){
      const cacheEntry = await cache.get(context.url);

      if (!cacheEntry){
        return;
      }
      // On cache hit we call all listening queries
      this.queryListeners.forEach((listener) => {
        listener(cacheEntry);
      })
      return cacheEntry;
    }
    else {
      throw new Error(`Unknown view mode passed to ${this.constructor.name}`)
    }
  }
}


export interface IActorContextPreprocessSetCacheQuerySourceArgs extends IActorContextPreprocessArgs {
  /**
   * The maximum number of triples in the cache.
   * @range {integer}
   * @default {124_000}
   */
  cacheSizeNumTriples: number;
}
