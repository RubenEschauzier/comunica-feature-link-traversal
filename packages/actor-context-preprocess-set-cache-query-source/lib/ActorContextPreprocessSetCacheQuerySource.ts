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
import { BufferedIterator, AsyncIterator} from 'asynciterator';
import type * as RDF from '@rdfjs/types';
import { IActionQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';

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
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    const context = action.context;
    const cacheManager = context.getSafe(KeysCaching.cacheManager);
    cacheManager.registerCache(
      CacheEntrySourceState.cacheSourceStateQuerySource,
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
  { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink } | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
  BindingsStream | AsyncIterator<RDF.Quad> | ISourceState
> {
  // List of active queries waiting for data
  private queryListeners: Array<(doc: ISourceState) => void> = [];

  public async construct(
    cache: IPersistentCache<ISourceState>,
    context: { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink} | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
  ): Promise<ISourceState | BindingsStream | AsyncIterator<RDF.Quad> | undefined> {
    if (context.mode === 'get'){
      console.log(context.url)
      const cacheEntry = await cache.get(context.url);
      if (!cacheEntry || !cacheEntry.cachePolicy?.satisfiesWithoutRevalidation(context.action)){
        return;
      }
      // On cache hit we call all listening queries
      this.queryListeners.forEach((listener) => {
        listener(cacheEntry);
      })
      return cacheEntry;
    }
    else if (context.mode === 'queryBindings'){
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        // Iterator we will push bindings to when this view is called with 'get'
        const result = new BufferedIterator<Bindings>({ autoStart: false });

        const onCacheGet = ( document: ISourceState ) => {
          if (result.closed) {
            return;
          }
          const bindingStream = document.source.queryBindings(context.operation, new ActionContext());
          bindingStream.on('data', (data) => {
            if (!result.closed) {
              (<any>result)._push(data)
            }
            else{
              console.log("CLOSED WHILE TRYING TO PUSH DATA")
            }
          });
          bindingStream.on('error', (error: Error) => {
             if (!result.closed) {
               result.emit('error', error)
             }
          });
        }

        this.queryListeners.push(onCacheGet);
        const cleanup = () => {
          const index = this.queryListeners.indexOf(onCacheGet);
          if (index > -1) {
            this.queryListeners.splice(index, 1);
          }
        };

        result.on('end', () => {
          cleanup();
          console.log("STream ended")
        });
        result.on('error', () => {
          cleanup();
          console.log("Stream error")
        })
        result.on('close', () => {
          cleanup();
          console.log("Stream closed")
        })

        return result;
      }
      else {
        throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
      }
    }
    else if (context.mode === 'queryQuads'){
      if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
        const result = new BufferedIterator<RDF.Quad>({ autoStart: false });
        const onCacheGet = ( document: ISourceState ) => {
          const bindingStream = document.source.queryQuads(context.operation, new ActionContext());
          bindingStream.on('data', (data: RDF.Quad) => (<any>result)._push(data));
        }
        this.queryListeners.push(onCacheGet);
        // this.addCleanup(result, onCacheGet);
        return result;
      }
      else {
        throw new Error(`${this.construct.name} does not support operations other than quad or triple patterns`);
      }
    }
    else {
      throw new Error(`Unknown view mode passed to ${this.constructor.name}: ${context.mode}`)
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
