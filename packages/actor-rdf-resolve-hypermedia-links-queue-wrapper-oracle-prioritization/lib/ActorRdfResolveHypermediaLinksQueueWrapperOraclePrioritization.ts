// eslint-disable-next-line import/no-nodejs-modules
import * as fs from 'node:fs';
import type {
  IActionRdfResolveHypermediaLinksQueue,
  IActorRdfResolveHypermediaLinksQueueOutput,
  MediatorRdfResolveHypermediaLinksQueue,
} from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { IActorArgs, IActorTest, TestResult } from '@comunica/core';
import { ActionContextKey, failTest, passTestVoid } from '@comunica/core';
import { LinkQueueOraclePrioritization } from './LinkQueueOraclePrioritization';
import { KeysInitQuery } from '@comunica/context-entries';
import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';

/**
 * A comunica Wrapper Limit Count RDF Resolve Hypermedia Links Queue Actor.
 */
export class ActorRdfResolveHypermediaLinksQueueWrapperOraclePrioritization extends
  ActorRdfResolveHypermediaLinksQueue {
  private readonly mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;

  public constructor(args: IActorRdfResolveHypermediaLinksQueueWrapperOraclePrioritizationArgs) {
    super(args);
  }

  public async readRccFile(): Promise<Record<string, Record<string, number>>> {
    const data = JSON.parse(fs.readFileSync("/tmp/oracle-data-input/oracleData.json", 'utf-8'));
    return data;
  }

  public async test(action: IActionRdfResolveHypermediaLinksQueue): Promise<TestResult<IActorTest>> {
    if (action.context.get(KEY_CONTEXT_WRAPPED)) {
      return failTest('Unable to wrap link queues multiple times');
    }
    return passTestVoid();
  }

  public async run(action: IActionRdfResolveHypermediaLinksQueue): Promise<IActorRdfResolveHypermediaLinksQueueOutput> {
    const context = action.context.set(KEY_CONTEXT_WRAPPED, true);
    const { linkQueue } = await this.mediatorRdfResolveHypermediaLinksQueue.mediate({ ...action, context });
    
    if (!(linkQueue instanceof LinkQueuePriority)) {
      throw new TypeError('Tried to wrap a non-priority queue with a link prioritisation wrapper.');
    }

    // Load oracle scores
    const rccScores = await this.readRccFile();
    const queryBase64 = btoa(action.context.getSafe(KeysInitQuery.queryString).trim());
    const rccScoresQuery = rccScores[queryBase64];

    if ( rccScoresQuery === undefined){
      throw new Error(`Unknown query: ${action.context.getSafe(KeysInitQuery.queryString).trim()}`);
    }
    return { linkQueue: new LinkQueueOraclePrioritization(linkQueue, rccScoresQuery) };
  }
}

export interface IActorRdfResolveHypermediaLinksQueueWrapperOraclePrioritizationArgs
  extends IActorArgs<IActionRdfResolveHypermediaLinksQueue, IActorTest, IActorRdfResolveHypermediaLinksQueueOutput> {
  mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
}

export const KEY_CONTEXT_WRAPPED = new ActionContextKey<boolean>(
  '@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-limit-count:wrapped',
);
