import { ActorContextPreprocess, IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal/lib/Keys';
import { TestResult, IActorTest, passTestVoid } from '@comunica/core';

/**
 * A comunica Set Dynamic Links Filter Context Preprocess Actor.
 */
export class ActorContextPreprocessSetDynamicLinksFilter extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(action: IActionContextPreprocess): Promise<TestResult<IActorTest>> {
    return passTestVoid(); // TODO implement
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    let context = action.context;

    // Set traverse flag to true if the flag is undefined.
    context = context.setDefault(KeysRdfResolveHypermediaLinks.dynamicFilter, new Set());
    
    return { context };
  }
}
