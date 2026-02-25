import type { IActorContextPreprocessOutput, IActorContextPreprocessArgs } from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysQuerySourceIdentify } from '@comunica/context-entries';
import { KeysDerivedResourceIdentify } from '@comunica/context-entries-link-traversal';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { ILinkTraversalManager } from '@comunica/types-link-traversal';

/**
 * A comunica Set Defaults Link Traversal Context Preprocess Actor.
 */
export class ActorContextPreprocessSetDefaultsLinkTraversal extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(_action: IAction): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IAction): Promise<IActorContextPreprocessOutput> {
    let context = action.context;

    // Set traverse flag to true if the flag is undefined.
    if (!context.has(KeysQuerySourceIdentify.traverse)) {
      context = context.set(KeysQuerySourceIdentify.traverse, true);
    }
    // Set derived resource container if it is undefined.
    if (!context.has(KeysDerivedResourceIdentify.derivedResourcesContainer)) {
      context = context.set(KeysDerivedResourceIdentify.derivedResourcesContainer, {});
    }

    return { context };
  }
}

// TODO: Temporary container for things I need to make the derived resources work,
// will make a separate actor when things are more clear what is needed
export interface IDerivedResourcesContainer {
  traversalManager?: ILinkTraversalManager
}