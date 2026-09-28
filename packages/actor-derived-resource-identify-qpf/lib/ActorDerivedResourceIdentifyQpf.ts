import { ActorDerivedResourceIdentify, IActionDerivedResourceIdentify, IActorDerivedResourceIdentifyOutput, IActorDerivedResourceIdentifyArgs } from '@comunica/bus-derived-resource-identify';
import { MediatorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import { TestResult, IActorTest, passTestVoid, failTest, ActionContext } from '@comunica/core';
import { ComunicaDataFactory, FragmentSelectorShape } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import * as path from 'node:path';
import { MediatorQuerySourceDereferenceLink } from '@comunica/bus-query-source-dereference-link';
import { KeysInitQuery } from '@comunica/context-entries';
import { LazyQuerySource } from './LazyQuerySource';
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
    const url = new URL(
       action.derivedResourceUnidentified.template,
       action.derivedResourceUnidentified.baseUrl
    ).href

    // A QPF interface answers any single pattern, so what it can answer is known without asking it.
    // The entry page is only dereferenced once the resource is actually queried: dereferencing it
    // here would cost every query over the pod a request, including those that never pick it
    const selectorShape: FragmentSelectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern: this.algebraFactory.createPattern(
          this.dataFactory.variable('s'),
          this.dataFactory.variable('p'),
          this.dataFactory.variable('o'),
          this.dataFactory.variable('g'),
        ),
      },
      variablesOptional: [
        this.dataFactory.variable('s'),
        this.dataFactory.variable('p'),
        this.dataFactory.variable('o'),
        this.dataFactory.variable('g'),
      ],
    };
    const querySource = new LazyQuerySource(url, selectorShape, async() =>
      (await this.mediatorQuerySourceDereferenceLink.mediate({
        link: { url },
        context: new ActionContext({ [KeysInitQuery.dataFactory.name]: this.dataFactory }),
      })).source);

    const derivedResource: IActorDerivedResourceIdentifyOutput = {
      derivedResourceIdentified: {
        iri: url,
        derivedResourceSelectorShape: selectorShape,
        ...action.derivedResourceUnidentified,
        querySource,
        resourceCoefficients:  {
          selectivity: 1,
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