import { ActorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type {
  IActionRdfResolveHypermediaLinksQueue,
  IActorRdfResolveHypermediaLinksQueueOutput,
  IActorRdfResolveHypermediaLinksQueueArgs,
  MediatorRdfResolveHypermediaLinksQueue,
} from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { KeysInitQuery } from '@comunica/context-entries';
import { KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import type { IActorTest, TestResult } from '@comunica/core';
import { ActionContextKey, failTest, passTestVoid } from '@comunica/core';
import type { IActionContext, QuerySourceUnidentified } from '@comunica/types';
import { LinkQueueWrapperKnownSources } from './LinkQueueWrapperKnownSources';

/**
 * Comunica hypermedia link queue actor that keeps traversal out of the namespaces of the known query sources.
 *
 * A query source given with a type, such as a SPARQL endpoint, is a known member of the federation, and
 * the only way its data is accessed. Links into the namespaces it covers, as declared in the context entry
 * `sourceNamespaces`, are not traversed, seeds included. The source itself is still taken up: link traversal
 * takes it up as a seed with its type forced, and its url lies outside the namespaces of its documents.
 */
export class ActorRdfResolveHypermediaLinksQueueWrapperKnownSources extends ActorRdfResolveHypermediaLinksQueue {
  private readonly mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;

  private static readonly keyWrapped = new ActionContextKey<boolean>(
    '@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-known-sources:wrapped',
  );

  public constructor(args: IActorRdfResolveHypermediaLinksQueueWrapperKnownSourcesArgs) {
    super(args);
    this.mediatorRdfResolveHypermediaLinksQueue = args.mediatorRdfResolveHypermediaLinksQueue;
  }

  /**
   * The urls of the query sources given with a type, which are the known sources. Whether they are taken up
   * as seeds of traversal or not, their data is accessed through their interface.
   */
  public static knownSourceUrls(context: IActionContext): string[] {
    const sources: QuerySourceUnidentified[] = context.get(KeysInitQuery.querySourcesUnidentified) ?? [];
    const urls: string[] = [];
    for (const source of sources) {
      if (typeof source !== 'string' && !('match' in source) && source.type !== undefined &&
        typeof source.value === 'string') {
        urls.push(source.value);
      }
    }
    return urls;
  }

  /**
   * The namespaces the given known sources cover. Declared namespaces of sources that are not known
   * sources of this query are ignored: their documents are not served otherwise.
   */
  public static knownNamespaces(context: IActionContext, sourceUrls: string[]): string[] {
    const declared = context.get(KeysRdfResolveHypermediaLinks.sourceNamespaces) ?? {};
    return sourceUrls.flatMap(url => declared[url] ?? []);
  }

  public async test(action: IActionRdfResolveHypermediaLinksQueue): Promise<TestResult<IActorTest>> {
    if (action.context.get(ActorRdfResolveHypermediaLinksQueueWrapperKnownSources.keyWrapped)) {
      return failTest('Unable to wrap link queues multiple times');
    }
    const Actor = ActorRdfResolveHypermediaLinksQueueWrapperKnownSources;
    if (Actor.knownNamespaces(action.context, Actor.knownSourceUrls(action.context)).length === 0) {
      return failTest('No namespaces of known query sources to keep traversal out of');
    }
    return passTestVoid();
  }

  public async run(action: IActionRdfResolveHypermediaLinksQueue): Promise<IActorRdfResolveHypermediaLinksQueueOutput> {
    const context = action.context.set(ActorRdfResolveHypermediaLinksQueueWrapperKnownSources.keyWrapped, true);
    const sourceUrls = ActorRdfResolveHypermediaLinksQueueWrapperKnownSources.knownSourceUrls(action.context);
    const namespaces = ActorRdfResolveHypermediaLinksQueueWrapperKnownSources
      .knownNamespaces(action.context, sourceUrls);
    this.logDebug(
      action.context,
      'Keeping traversal out of the namespaces of known sources',
      () => ({ sourceUrls, namespaces }),
    );
    const { linkQueue } = await this.mediatorRdfResolveHypermediaLinksQueue.mediate({ ...action, context });
    return {
      linkQueue: new LinkQueueWrapperKnownSources(linkQueue, namespaces),
    };
  }
}

export interface IActorRdfResolveHypermediaLinksQueueWrapperKnownSourcesArgs extends
  IActorRdfResolveHypermediaLinksQueueArgs {
  /**
   * The link queue mediator, producing the queue to wrap.
   */
  mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
}
