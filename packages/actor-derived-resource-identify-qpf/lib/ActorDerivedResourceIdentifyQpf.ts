import { ActorDerivedResourceIdentify, IActionDerivedResourceIdentify, IActorDerivedResourceIdentifyOutput, IActorDerivedResourceIdentifyArgs } from '@comunica/bus-derived-resource-identify';
import { MediatorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import { TestResult, IActorTest, passTestVoid, failTest, ActionContext } from '@comunica/core';
import { ComunicaDataFactory } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import * as path from 'node:path';
import { MediatorQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { KeysInitQuery } from '@comunica/context-entries';
/**
 * A comunica Qpf Derived Resource Identify Actor.
 */
export class ActorDerivedResourceIdentifyQpf extends ActorDerivedResourceIdentify {
  protected dataFactory: ComunicaDataFactory = new DataFactory();
  protected algebraFactory: AlgebraFactory = new AlgebraFactory(this.dataFactory);

  protected readonly mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;

  public constructor(args: IActorDerivedResourceIdentifyQpfArgs) {
    super(args);
    this.mediatorQuerySourceDereferenceLink = args.mediatorQuerySourceDereferenceLink;
  }

  public async test(action: IActionDerivedResourceIdentify): Promise<TestResult<IActorTest>> {
    if (action.derivedResourceUnidentified.filter !== 'qpf'){
      return failTest(`${this.name} can only identify qpf derived resources`);
    }
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourceIdentify): Promise<IActorDerivedResourceIdentifyOutput> {
    const url = path.join(
      action.derivedResourceUnidentified.baseUrl,
      action.derivedResourceUnidentified.template
    );
    const querySourceQpf = await this.mediatorQuerySourceDereferenceLink.mediate({
      link: { url },
      context: new ActionContext({[KeysInitQuery.dataFactory.name]: this.dataFactory })
    });
    const derivedResource: IActorDerivedResourceIdentifyOutput = {
      derivedResourceIdentified: {
        iri: url,
        derivedResourceSelectorShape: await querySourceQpf.source.getSelectorShape(
          new ActionContext()
        ),
        ...action.derivedResourceUnidentified,
        querySource: querySourceQpf.source,
        resourceCoefficients:  {
          selectivty: 1,
          requests: 10,
          compute: 1
        }        
      }
    }
    return derivedResource;
  }
}

export interface IActorDerivedResourceIdentifyQpfArgs extends IActorDerivedResourceIdentifyArgs {
  mediatorQuerySourceDereferenceLink: MediatorQuerySourceDereferenceLink;
}