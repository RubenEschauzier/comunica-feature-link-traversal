import { ActorContextPreprocess, IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { KeysQueryOperation } from '@comunica/context-entries';
import { KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, passTestVoid } from '@comunica/core';

/**
 * A comunica Set Source Tracking Context Preprocess Actor.
 */
export class ActorContextPreprocessSetSourceTracking extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(action: IActionContextPreprocess): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    let context = action.context;
    context = context.set(KeysQueryOperation.unionDefaultGraph, true)
                     .set(KeysRdfResolveHypermediaLinks.annotateSources, "graph");

    return { context }; 
  }
}
