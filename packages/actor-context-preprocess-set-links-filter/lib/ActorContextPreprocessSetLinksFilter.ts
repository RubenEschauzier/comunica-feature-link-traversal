import { ActorContextPreprocess, IActionContextPreprocess, IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, passTestVoid } from '@comunica/core';
import { ILink } from '@comunica/types';

/**
 * A comunica Set Links Filter Context Preprocess Actor.
 */
export class ActorContextPreprocessSetLinksFilter extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(action: IActionContextPreprocess): Promise<TestResult<IActorTest>> {
    return passTestVoid(); // TODO implement
  }

  public async run(action: IActionContextPreprocess): Promise<IActorContextPreprocessOutput> {
    let context = action.context;

    // Set link filter that excludes any links with /filters. This is a hack to ensure Comunica
    // works in SolidBench setup of derived resources.
    context = context.setDefault(KeysRdfResolveHypermediaLinks.linkFilters, 
      [ (link: ILink) => !link.url.includes('/filters') ]
    );    

    return { context };
  }
}
