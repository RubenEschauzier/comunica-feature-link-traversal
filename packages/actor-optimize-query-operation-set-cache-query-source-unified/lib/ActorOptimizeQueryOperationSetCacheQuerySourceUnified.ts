// Import { ActorOptimizeQueryOperation, IActionOptimizeQueryOperation, IActorOptimizeQueryOperationOutput, IActorOptimizeQueryOperationArgs } from '@comunica/bus-optimize-query-operation';
// import { TestResult, IActorTest, passTestVoid, ActionContextKey, ActionContext } from '@comunica/core';

// import type * as RDF from '@rdfjs/types';
// import { PassThrough } from 'readable-stream';
// import { BindingsStream, ICacheView, IPersistentCache, ISetFn, ISourceState } from '@comunica/types';
// import { IActionQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
// import { Algebra, AlgebraFactory, isKnownOperation } from '@comunica/utils-algebra';
// import { UnifiedStoreCache, UnifiedStoreCacheWrapper } from './UnifiedStoreCache';
// import { AsyncReiterableArray } from 'asyncreiterable';
// import { KeysCaching, KeysQueryOperation, KeysQuerySourceIdentify } from '@comunica/context-entries';
// import { PendingStreamsIndex } from "rdf-streaming-store";

// import { DataFactory } from 'rdf-data-factory';
// import { AsyncIterator } from 'asynciterator';
// import { BindingsFactory } from '@comunica/utils-bindings-factory';
// import { quadsToBindings } from '@comunica/bus-query-source-identify';

// /**
//  * A comunica Set Cache Query Source Unified Optimize Query Operation Actor.
//  */
// export class ActorOptimizeQueryOperationSetCacheQuerySourceUnified extends ActorOptimizeQueryOperation {
//   protected unifiedStoreCache: UnifiedStoreCache;
//   protected unifiedStoreCacheWrapper: UnifiedStoreCacheWrapper;

//   protected readonly cacheSizeNumTriples: number;

//   public constructor(args: IActorOptimizeQueryOperationSetCacheQuerySourceUnifiedArgs) {
//     super(args);
//     this.unifiedStoreCache = new UnifiedStoreCache(args.cacheSizeNumTriples);
//     this.unifiedStoreCacheWrapper = new UnifiedStoreCacheWrapper(this.unifiedStoreCache);
//     this.cacheSizeNumTriples = args.cacheSizeNumTriples;
//   }

//   public async test(action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
//     return passTestVoid();
//   }

//   public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
//     const context = action.context;
//     if (!action.context.get(KeysQuerySourceIdentify.traverse)){
//       return { context, operation: action.operation };
//     }
//     //TODO: Same as context preprocess
//     if (context.get(KeysCaching.clearCache) || context.get(new ActionContextKey('clearCache'))) {
//       console.log("Cleaned cache.")
//       this.unifiedStoreCache = new UnifiedStoreCache(this.cacheSizeNumTriples);
//     }

//     const cacheManager = context.getSafe(KeysCaching.cacheManager);
//     cacheManager.registerCache(
//       CacheEntrySourceState.cacheSourceStateQuerySourceBloomFilter,
//       this.UnifiedStoreCache,
//       new SetSourceStateCache(),
//     );

//     cacheManager.registerCacheView(
//       CacheSourceStateViews.cacheQueryViewBloomFilter,
//       new GetUnifiedCacheView(),
//     );

//     return { context, operation: action.operation };

//   }
// }

// export class SetSourceStateCache implements ISetFn<ISourceState, AsyncIterator<RDF.Quad>, { headers: Headers }> {
//   protected DF: DataFactory = new DataFactory();
//   protected AF: AlgebraFactory = new AlgebraFactory(this.DF);

//   public async setInCache(
//     key: string,
//     value: ISourceState,
//     cache: IPersistentCache<AsyncIterator<RDF.Quad>>,
//     context: { headers: Headers },
//   ): Promise<void> {
//     // Construct a universal match pattern to drain the source
//     const pattern = this.AF.createPattern(
//       this.DF.variable('s'),
//       this.DF.variable('p'),
//       this.DF.variable('o'),
//       this.DF.variable('g'),
//     );

//     // queryQuads directly returns the AsyncIterator<RDF.Quad> required by your wrapper
//     const quadStream = value.source.queryQuads(pattern, new ActionContext());

//     await cache.set(key, quadStream);
//   }
// }

// export class QueryUnifiedCacheView implements ICacheView<
//   ISourceState,
//   { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink } | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
//   AsyncIterator<BindingsStream> | AsyncIterator<AsyncIterator<RDF.Quad>> | AsyncIterator<RDF.Quad>
// > {
//   protected pendingStreams: PendingStreamsIndex<RDF.Quad> = new PendingStreamsIndex();
//   protected releasedUrls: Set<string> = new Set();

//   protected ended = false;
//   protected pendingCount = 0;

//   protected unifiedStore: UnifiedStoreCache;
//   protected DF: DataFactory = new DataFactory();
//   protected BF: BindingsFactory = new BindingsFactory(this.DF, {});

//   public constructor(unifiedStore: UnifiedStoreCache) {
//     this.unifiedStore = unifiedStore;
//   }

//   protected checkForTermination(): void {
//     if (this.ended && this.pendingCount === 0) {
//       for (const stream of this.pendingStreams.allStreams) {
//         stream.push(null);
//       }
//     }
//   }

//   protected getTermOrUndefined(term: RDF.Term): RDF.Term | undefined {
//     return term.termType === 'Variable' ? undefined : term;
//   }

//   public async construct(
//     cache: IPersistentCache<AsyncIterator<RDF.Quad>>,
//     context: { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink} | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
//   ): Promise<any> {

//     if (context.mode === 'get') {
//       if (context.url === 'end') {
//         if (!this.ended) {
//           this.ended = true;
//           this.checkForTermination();
//         }
//         return;
//       }

//       this.pendingCount++;
//       const cachedQuads = await cache.get(context.url);

//       // Return a dummy ISourceState to satisfy engine requirements if necessary
//       return cachedQuads
//     }

//     else if (context.mode === 'queryQuads' || context.mode === 'queryBindings') {
//       if (context.operation.type === 'pattern') {
//         const pattern = context.operation as Algebra.Pattern;

//         const s = this.getTermOrUndefined(pattern.subject);
//         const p = this.getTermOrUndefined(pattern.predicate);
//         const o = this.getTermOrUndefined(pattern.object);
//         const g = this.getTermOrUndefined(pattern.graph);

//         this.pendingStreams.addPatternListener(stream, s, p, o, g);

//         const  = this.unifiedStore.matchReleased(s, p, o, g, this.releasedUrls);

//         if (context.mode === 'queryBindings') {
//           const bindingsStream = quadsToBindings(
//             stream,
//             pattern,
//             this.DF,
//             this.BF,
//             true
//           );

//           const bindingsStream = this.quadsToBindings(stream, pattern);
//           return AsyncReiterableArray.fromFixed([bindingsStream]).iterator();
//         }

//         // Must wrap Node.js stream in AsyncIterator for queryQuads return type
//         return AsyncReiterableArray.fromFixed([stream]).iterator();
//       } else {
//         throw new Error(`GetSourceStateCacheView only supports pattern operations.`);
//       }
//     }
//     else {
//       throw new Error(`Unknown view mode.`);
//     }
//   }
// }

// export class GetUnifiedCacheView implements ICacheView<
//   ISourceState,
//   { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink } | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
//   AsyncIterator<BindingsStream> | AsyncIterator<AsyncIterator<RDF.Quad>> | ISourceState
// > {
//   protected pendingStreams: PendingStreamsIndex<RDF.Quad> = new PendingStreamsIndex();
//   protected releasedUrls: Set<string> = new Set();

//   protected ended = false;
//   protected pendingCount = 0;

//   public constructor() {
//   }

//   protected checkForTermination(): void {
//     if (this.ended && this.pendingCount === 0) {
//       for (const stream of this.pendingStreams.allStreams) {
//         stream.push(null);
//       }
//     }
//   }

//   /**
//    * Converts variable terms in the pattern to undefined for the rdf-stores matcher.
//    */
//   protected getTermOrUndefined(term: RDF.Term): RDF.Term | undefined {
//     return term.termType === 'Variable' ? undefined : term;
//   }

//   public async construct(
//     cache: IPersistentCache<ISourceState>,
//     context: { url: string, mode: 'get', action: IActionQuerySourceDereferenceLink} | { mode: 'queryBindings' | 'queryQuads', operation: Algebra.Operation},
//   ): Promise<AsyncIterator<BindingsStream> | AsyncIterator<AsyncIterator<RDF.Quad>> | ISourceState | undefined> {

//     if (context.mode === 'get') {
//       if (context.url === 'end') {
//         if (!this.ended) {
//           this.ended = true;
//           this.checkForTermination();
//         }
//         return;
//       }

//       this.pendingCount++;
//       try {
//         const cacheEntry = await cache.get(context.url);

//         if (cacheEntry && cacheEntry.cachePolicy?.satisfiesWithoutRevalidation(context.action)) {
//           if (!this.releasedUrls.has(context.url)) {
//             this.releasedUrls.add(context.url);

//             const cachedQuads = cache.getQuadsByUrl(context.url);
//             if (cachedQuads) {
//               for (const quad of cachedQuads) {
//                 const waitingStreams = this.pendingStreams.getPendingStreamsForQuad(quad);
//                 for (const stream of waitingStreams) {
//                   stream.push(quad);
//                 }
//               }
//             }
//           }
//           return cacheEntry;
//         }
//       } finally {
//         this.pendingCount--;
//         this.checkForTermination();
//       }
//     }

//     else if (context.mode === 'queryQuads' || context.mode === 'queryBindings') {
//       if (isKnownOperation(context.operation, Algebra.Types.PATTERN)) {
//         const pattern = context.operation as Algebra.Pattern;
//         const stream = new PassThrough({ objectMode: true });

//         const s = this.getTermOrUndefined(pattern.subject);
//         const p = this.getTermOrUndefined(pattern.predicate);
//         const o = this.getTermOrUndefined(pattern.object);
//         const g = this.getTermOrUndefined(pattern.graph);

//         this.pendingStreams.addPatternListener(stream, s, p, o, g);

//         const existingMatches = this.unifiedStore.matchReleased(s, p, o, g, this.releasedUrls);
//         for (const match of existingMatches) {
//           stream.push(match);
//         }

//         if (context.mode === 'queryBindings') {
//           const bindingsStream = this.quadsToBindings(stream, pattern);
//           return AsyncReiterableArray.fromInitialData([bindingsStream]).iterator();
//         }

//         return AsyncReiterableArray.fromInitialData([stream]).iterator();
//       } else {
//         throw new Error(`${this.constructor.name} only supports pattern operations.`);
//       }
//     }

//     else {
//       throw new Error(`Unknown view mode: ${context.mode}`);
//     }
//   }

//   protected quadsToBindings(quadStream: NodeJS.ReadableStream, pattern: Algebra.Pattern): BindingsStream {
//     throw new Error('Method not implemented.');
//   }
// }

// export interface IActorOptimizeQueryOperationSetCacheQuerySourceUnifiedArgs extends IActorOptimizeQueryOperationArgs {
//     /**
//    * The maximum number of triples in the cache.
//    * @range {integer}
//    * @default {124000}
//    */
//   cacheSizeNumTriples: number;
// }
