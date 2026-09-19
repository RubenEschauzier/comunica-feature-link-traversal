import type { IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysQueryOperation } from '@comunica/context-entries';
import { KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import type { TestResult, IActorTest } from '@comunica/core';
import { passTestVoid } from '@comunica/core';

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
    // `index` keeps the document a quad came from beside the aggregated store rather than in the
    // graph of the quad. Under `graph` one triple asserted by two documents is two quads, and so
    // two identical bindings that the joins go on to multiply
    context = context.set(KeysQueryOperation.unionDefaultGraph, true)
      .setDefault(KeysRdfResolveHypermediaLinks.annotateSources, 'index');

    return { context };
  }
}
