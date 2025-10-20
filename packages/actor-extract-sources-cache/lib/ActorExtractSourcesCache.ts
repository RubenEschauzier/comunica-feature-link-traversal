import type { IActionExtractSources, IActorExtractSourcesOutput, IActorExtractSourcesArgs } from '@comunica/bus-extract-sources';
import { ActorExtractSources } from '@comunica/bus-extract-sources';
import { KeysCaches } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type { IQuerySource, ISourceState } from '@comunica/types';

/**
 * A comunica Cache Extract Sources Actor.
 */
export class ActorExtractSourcesCache extends ActorExtractSources {
  public constructor(args: IActorExtractSourcesArgs) {
    super(args);
  }

  public async test(_action: IActionExtractSources): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionExtractSources): Promise<IActorExtractSourcesOutput> {
    const sourceCache = action.context.get(KeysCaches.storeCache);
    const sources: IQuerySource[] = [];
    if (sourceCache && sourceCache.size > 0) {
      for (const key of sourceCache.keys()) {
        const sourceState: ISourceState = sourceCache.get(key)!;
        sources.push(sourceState.source);
      }
    }
    return { sources };
  }
}
