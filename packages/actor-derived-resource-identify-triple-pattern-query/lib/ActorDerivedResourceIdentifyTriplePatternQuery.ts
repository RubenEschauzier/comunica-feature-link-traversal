import { ActorDerivedResourceIdentify, IActionDerivedResourceIdentify, IActorDerivedResourceIdentifyOutput, IActorDerivedResourceIdentifyArgs } from '@comunica/bus-derived-resource-identify';
import { MediatorQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { TestResult, IActorTest, passTestVoid, failTest, ActionContext } from '@comunica/core';
import * as path from 'node:path';

/**
 * A comunica Triple Pattern Query Derived Resource Identify Actor.
 */
export class ActorDerivedResourceIdentifyTriplePatternQuery extends ActorDerivedResourceIdentify {
  protected readonly mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;

  public constructor(args: IActorDerivedResourceIdentifyTriplePatternQueryArgs) {
    super(args);
    this.mediatorQuerySourceDereferenceLink = args.mediatorQuerySourceDereferenceLink;
  }

  public async test(action: IActionDerivedResourceIdentify): Promise<TestResult<IActorTest>> {
    return failTest(`${this.name}: not yet implemented`); 
  }

  public async run(action: IActionDerivedResourceIdentify): Promise<IActorDerivedResourceIdentifyOutput> {
    //TODO: Implement this for testing, this is QPF implementation
    const url = path.join(
      action.derivedResourceUnidentified.baseUrl,
      action.derivedResourceUnidentified.template
    );
    const querySourceQpf = await this.mediatorQuerySourceDereferenceLink.mediate({
      link: { url },
      context: new ActionContext()
    });

    const derivedResource: IActorDerivedResourceIdentifyOutput = {
      derivedResourceIdentified: {
        ...action.derivedResourceUnidentified,
        querySource: querySourceQpf.source,
        resourceCoefficients:  {
          selecitivty: 1,
          requests: 1,
          compute: 10
        }        
      }
    }

    return derivedResource;
  }
}

export interface IActorDerivedResourceIdentifyTriplePatternQueryArgs 
extends IActorDerivedResourceIdentifyArgs {
  mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;
}