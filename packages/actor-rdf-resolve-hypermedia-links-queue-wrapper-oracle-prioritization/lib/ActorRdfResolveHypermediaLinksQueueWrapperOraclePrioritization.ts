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
import { KeysInitQuery, KeysQuerySourceIdentify } from '@comunica/context-entries';
import { KeysStatisticsTraversal } from '@comunica/context-entries-link-traversal';

/**
 * A comunica Wrapper Limit Count RDF Resolve Hypermedia Links Queue Actor.
 */
export class ActorRdfResolveHypermediaLinksQueueWrapperOraclePrioritization extends
  ActorRdfResolveHypermediaLinksQueue {
  private readonly mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
  private readonly rccScores: Promise<Record<string, Record<string, number>>>;


  public constructor(args: IActorRdfResolveHypermediaLinksQueueWrapperOraclePrioritizationArgs) {
    super(args);
    this.rccScores = this.readRccFile();
  }

  public async readRccFile(): Promise<Record<string, Record<string, number>>> {
    const data = JSON.parse(fs.readFileSync("/tmp/r3-metric-output/oracleData.json", 'utf-8'));
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
    
    // Load oracle scores
    const rccScores = await this.rccScores;
    const queryBase64 = btoa(action.context.getSafe(KeysInitQuery.queryString).trim());
    const rccScoresQuery = rccScores[queryBase64];

    if ( rccScoresQuery === undefined){
      console.log(`Unknown query: ${action.context.getSafe(KeysInitQuery.queryString).trim()}`);     
    }
    // const rccScoresQuery = {
    //   "https://solidbench.linkeddatafragments.org/pods/00000000000000000933/posts/2012-06-03": 1,
    //   "https://solidbench.linkeddatafragments.org/pods/00000000000000000933/posts/2011-08-17": 1,
    //   "https://solidbench.linkeddatafragments.org/pods/00000000000000000933/posts/2012-07-20": 1,
    //   "https://solidbench.linkeddatafragments.org/pods/00000000000000000933/posts/2012-06-07": 1,
    //   "https://solidbench.linkeddatafragments.org/pods/00000000000000000933/posts/2010-02-14": 1,
    //   "https://solidbench.linkeddatafragments.org/pods/00000000000000000933/posts/2011-01-05": 1
    // }
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
