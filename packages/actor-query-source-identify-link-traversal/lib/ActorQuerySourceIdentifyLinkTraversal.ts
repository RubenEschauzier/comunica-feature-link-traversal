import type {
  IActionQuerySourceIdentify,
  IActorQuerySourceIdentifyOutput,
  IActorQuerySourceIdentifyArgs,
} from '@comunica/bus-query-source-identify';
import { ActorQuerySourceIdentify } from '@comunica/bus-query-source-identify';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import type { TestResult, IActorTest } from '@comunica/core';
import { passTestVoid, failTest } from '@comunica/core';
import type { Algebra } from '@comunica/utils-algebra';
import { QuerySourceLinkTraversal } from './QuerySourceLinkTraversal';
import { CacheKey, ICacheKey, IViewKey, ViewKey } from '@comunica/cache-manager-entries';

/**
 * A comunica Link Traversal Query Source Identify Actor.
 */
export class ActorQuerySourceIdentifyLinkTraversal extends ActorQuerySourceIdentify {
  protected readonly cacheEntryKey?: ICacheKey<unknown, unknown, unknown>;
  protected readonly cacheViewKey?: IViewKey<unknown, unknown, unknown>;
  protected readonly cacheCountViewKey?: IViewKey<unknown, {operation: Algebra.Operation; [key: string]: any }, number>;
  protected readonly setCardinalityFromCacheMinLimit: number;

  public constructor(args: IActorQuerySourceIdentifyLinkTraversalArgs) {
    super(args);
    if (args.cacheEntryName && args.cacheViewName){
      this.cacheEntryKey = new CacheKey(args.cacheEntryName);
      this.cacheViewKey = new ViewKey(args.cacheViewName);
    }
    if (args.cacheCountViewName){
      this.cacheCountViewKey = new ViewKey(args.cacheCountViewName);
    }
    this.setCardinalityFromCacheMinLimit = args.setCardinalityFromCacheMinLimit;
  }

  public async test(action: IActionQuerySourceIdentify): Promise<TestResult<IActorTest>> {
    const source = action.querySourceUnidentified;
    if (source.type !== undefined && source.type !== 'traverse') {
      return failTest(`${this.name} requires a single query source with traverse type to be present in the context.`);
    }
    if (!action.querySourceUnidentified.context?.has(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager)) {
      return failTest(`${this.name} requires a single query source with a link traversal manager to be present in the context.`);
    }
    return passTestVoid();
  }

  public async run(action: IActionQuerySourceIdentify): Promise<IActorQuerySourceIdentifyOutput> {
    const querySourceContext = action.querySourceUnidentified.context!;
    const linkTraversalManager = querySourceContext.getSafe(KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager);
    return {
      querySource: {
        source: new QuerySourceLinkTraversal(
          linkTraversalManager,
          this.cacheEntryKey,
          this.cacheViewKey,
          this.cacheCountViewKey,
          this.setCardinalityFromCacheMinLimit,
        ),
        context: querySourceContext,
      },
    };
  }
}

export interface IActorQuerySourceIdentifyLinkTraversalArgs extends IActorQuerySourceIdentifyArgs {
  cacheEntryName?: string;
  cacheViewName?: string;
  cacheCountViewName?: string;
  /**
   * If the cardinality of queryBinding calls should be obtained from the
   * cache. If undefined the normal procedure for cardinality estimation will be used.
   * @range {integer}
   * @default {20000}
   */

  setCardinalityFromCacheMinLimit: number;
}