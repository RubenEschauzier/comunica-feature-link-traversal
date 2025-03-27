import type {
  IActionExtractSources,
  IActorExtractSourcesOutput,
  IActorExtractSourcesArgs,
} from '@comunica/bus-extract-sources';
import {
  ActorExtractSources,
} from '@comunica/bus-extract-sources';
import { KeysCaches } from '@comunica/context-entries';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type { IQuerySource, ISourceState } from '@comunica/types';

/**
 * A comunica Cache Extract Sources Actor.
 */
export class ActorExtractSourcesCache extends ActorExtractSources {
  public constructor(args: IActorExtractSourcesArgs) {
    super(args);
  }

  public async test(_action: IAction): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionExtractSources): Promise<IActorExtractSourcesOutput> {
    const sources: IQuerySource[] = [];
    const storeCache = action.context.getSafe(KeysCaches.storeCache);
    // Seed document
    const withinQueryCache = action.context.getSafe(KeysCaches.withinQueryStoreCache);

    for (const key of [ ...storeCache.keys(), ...withinQueryCache.keys() ]) {
      const testSource = storeCache.get(key)!;
      if (testSource) {
        const innerSource = testSource.source;
        try {
          sources.push(innerSource);
        } catch (err) {
          console.log(err);
        }
      }
    }
    const filteredSources = sources.filter((source) => {
      if (!Object.values((<any>source).source.indexesWrapped).every(wrapped => (<any>wrapped).index.features.sampling)) {
        console.log('Found RDFStore without sampling');
        return false;
      }
      return true;
    });
    if (filteredSources.length <= 5) {
      // Too little sources means we have an empty cache except for seed documents.
      return { sources: []};
    }
    return { sources: filteredSources };
  }

  public extractSource(cachedSource: ISourceState, depth = 0): IQuerySource | null {
    const anySource = <any> cachedSource;
    if (!cachedSource) {
      return null;
    };
    // If object has a direct 'source' field, return it
    if (anySource.source && // Console.log("HAs source attribute")
      anySource.source.constructor.name === 'RdfStore') {
      // Console.log("IS rdf store");
      return anySource.source;
    }
    return this.extractSource(anySource.source, depth + 1);
  }
}
