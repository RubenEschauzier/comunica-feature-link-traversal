import type { MediatorDereference } from '@comunica/bus-dereference';
import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { IActionOptimizeQueryOperation, IActorOptimizeQueryOperationOutput } from '@comunica/bus-optimize-query-operation';
import { ActorOptimizeQueryOperation } from '@comunica/bus-optimize-query-operation';
import { KeysInitQuery } from '@comunica/context-entries';
import { KeysDerivedResourceIdentify } from '@comunica/context-entries-link-traversal';
import type { IActorArgs, TestResult, IActorTest } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type { IDerivedResourceUnidentified } from './ActorExtractLinksSolidDerivedResources';
import { DerivedResourceDescriptionFetcher } from './DerivedResourceDescriptionFetcher';

/**
 * A comunica Prefetch Derived Resources Optimize Query Operation Actor.
 *
 * A pod's derived resources are only found once a document of it has been fetched and read, after
 * which their description and its filters take two more requests before anything can be asked of
 * them. For the seeds that wait is avoidable: once the seeds are known and before traversal starts, the description of every
 * container a seed lies in is requested, filters included, so it is at hand, or at least underway,
 * by the time the seed names it. Descriptions that do not exist cost a request that fails, sent
 * alongside the seed rather than after it.
 */
export class ActorOptimizeQueryOperationPrefetchDerivedResources extends ActorOptimizeQueryOperation {
  protected readonly descriptionFetcher: DerivedResourceDescriptionFetcher;

  public constructor(args: IActorOptimizeQueryOperationPrefetchDerivedResourcesArgs) {
    super(args);
    this.descriptionFetcher = new DerivedResourceDescriptionFetcher(
      args.mediatorDereferenceRdf,
      args.mediatorDereference,
    );
  }

  public async test(_action: IActionOptimizeQueryOperation): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    const seeds = this.seedUrls(action.context.get(KeysInitQuery.querySourcesUnidentified) ?? []);
    if (seeds.length === 0) {
      return { ...action, context: action.context };
    }

    const descriptions = action.context.get(KeysDerivedResourceIdentify.derivedResourceDescriptions) ??
      new Map<string, Promise<IDerivedResourceUnidentified[]>>();
    const context = action.context.set(KeysDerivedResourceIdentify.derivedResourceDescriptions, descriptions);
    for (const url of seeds.flatMap(seed => this.candidateDescriptions(seed))) {
      if (!descriptions.has(url)) {
        // A candidate that is not there is simply a pod without derived resources at that level
        descriptions.set(url, this.descriptionFetcher.fetch(url, context).catch(() => []));
      }
    }
    return { ...action, context };
  }

  /**
   * The urls among the query sources, which are the seeds of traversal.
   */
  protected seedUrls(sources: unknown[]): string[] {
    const urls: string[] = [];
    for (const source of sources) {
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
   * The Dereference RDF mediator
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * The Dereference mediator
   */
  mediatorDereference: MediatorDereference;
}
