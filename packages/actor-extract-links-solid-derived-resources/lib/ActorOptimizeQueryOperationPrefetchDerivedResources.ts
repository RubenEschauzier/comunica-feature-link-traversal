import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { MediatorDerivedResourceDiscover } from '@comunica/bus-derived-resource-discover';
import { ActorDerivedResourceDiscover } from '@comunica/bus-derived-resource-discover';
import type { IActionOptimizeQueryOperation, IActorOptimizeQueryOperationOutput } from '@comunica/bus-optimize-query-operation';
import { ActorOptimizeQueryOperation } from '@comunica/bus-optimize-query-operation';
import { KeysInitQuery } from '@comunica/context-entries';
import { KeysDerivedResourceIdentify } from '@comunica/context-entries-link-traversal';
import type { IActorArgs, TestResult, IActorTest } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { AlgebraFactory } from '@comunica/utils-algebra';
import type { IDerivedResourceUnidentified } from './ActorExtractLinksSolidDerivedResources';
import { UnionPushdown } from './UnionPushdown';

/**
 * A comunica Prefetch Derived Resources Optimize Query Operation Actor.
 * Prefetches the .meta files of the seed documents to quickly start the derived resource process
 *
 * A pod's derived resources are only found once a document of it has been fetched and read, after
 * which their description and its filters take two more requests before anything can be asked of
 * them. For the seeds that wait is avoidable: once the seeds are known and before traversal starts, the description of every
 * container a seed lies in is requested, filters included, so it is at hand, or at least underway,
 * by the time the seed names it. Descriptions that do not exist cost a request that fails, sent
 * alongside the seed rather than after it.
 *
 * The candidates are guessed from the seeds' paths (the `.meta` of every container above them), which
 * assumes Solid's url layout. Where documents link to their dataset instead (`void:inDataset`), set
 * `prefetchDescriptions` to false: the query is still rewritten, and the dataset is found through the link.
 */
export class ActorOptimizeQueryOperationPrefetchDerivedResources extends ActorOptimizeQueryOperation {
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  public readonly mediatorDerivedResourceDiscover: MediatorDerivedResourceDiscover;
  public readonly prefetchDescriptions: boolean;
  // TODO: move into an optimize-query-operation actor of its own
  protected readonly unionPushdown = new UnionPushdown(new AlgebraFactory());

  public constructor(args: IActorOptimizeQueryOperationPrefetchDerivedResourcesArgs) {
    super(args);
    this.mediatorDereferenceRdf = args.mediatorDereferenceRdf;
    this.mediatorDerivedResourceDiscover = args.mediatorDerivedResourceDiscover;
    this.prefetchDescriptions = args.prefetchDescriptions ?? true;
  }

  public async test(_action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    // The query is rewritten before the traversal it starts takes it from the context, so that what is
    // delegated to derived resources is planned on the same operation that is executed
    const operation = this.unionPushdown.rewrite(action.operation);
    action = { ...action, operation, context: action.context.set(KeysInitQuery.query, operation) };

    const seeds = this.prefetchDescriptions ?
      this.seedUrls(action.context.get(KeysInitQuery.querySourcesUnidentified) ?? []) :
      [];
    if (seeds.length === 0) {
      return { ...action, context: action.context };
    }

    const descriptions = action.context.get(KeysDerivedResourceIdentify.derivedResourceDescriptions) ??
      new Map<string, Promise<IDerivedResourceUnidentified[]>>();
    const context = action.context.set(KeysDerivedResourceIdentify.derivedResourceDescriptions, descriptions);
    for (const url of seeds.flatMap(seed => this.candidateDescriptions(seed))) {
      if (!descriptions.has(url)) {
        // A candidate that is not there is simply a pod without derived resources at that level
        descriptions.set(url, ActorDerivedResourceDiscover.discoverDescription(
          url,
          context,
          this.mediatorDereferenceRdf,
          this.mediatorDerivedResourceDiscover,
        ).catch(() => []));
      }
    }
    return { ...action, context };
  }

  /**
   * The urls among the query sources, which are the seeds of traversal. A source given with a type, such
   * as a SPARQL endpoint, is queried as that type rather than traversed, so it is no seed: its url is not
   * a document in a container that could be described.
   */
  protected seedUrls(sources: unknown[]): string[] {
    const urls: string[] = [];
    for (const source of sources) {
      if (typeof source === 'object' && source !== null && (<{ type?: unknown }> source).type !== undefined) {
        continue;
      }
      const value = typeof source === 'string' ? source : (<{ value?: unknown }> source)?.value;
      if (typeof value === 'string' && /^https?:\/\//u.test(value)) {
        urls.push(value);
      }
    }
    return urls;
  }

  /**
   * Where a description covering the seed could be: the `.meta` of each container the seed lies in.
   * Which of them is the pod is not known before the seed is read.
   */
  protected candidateDescriptions(seed: string): string[] {
    const url = new URL(seed);
    url.hash = '';
    url.search = '';
    const segments = url.pathname.split('/');
    // The last segment is the document itself, which is not a container
    segments.pop();
    const candidates: string[] = [];
    while (segments.length > 0) {
      candidates.push(`${url.origin}${segments.join('/')}/.meta`);
      segments.pop();
    }
    return candidates;
  }
}

export interface IActorOptimizeQueryOperationPrefetchDerivedResourcesArgs
  extends IActorArgs<IActionOptimizeQueryOperation, IActorTest, IActorOptimizeQueryOperationOutput> {
  /**
   * The Dereference RDF mediator fetching the descriptions.
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * The discover mediator finding the resources in a description.
   */
  mediatorDerivedResourceDiscover: MediatorDerivedResourceDiscover;
  /**
   * Whether to fetch the descriptions of the containers above the seeds ahead of traversal. Off where
   * documents link to their dataset (`void:inDataset`) rather than the url layout naming it.
   * @default {true}
   */
  prefetchDescriptions?: boolean;
}
