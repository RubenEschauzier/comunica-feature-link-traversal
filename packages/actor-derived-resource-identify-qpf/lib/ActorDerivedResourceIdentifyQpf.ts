import { ActorDerivedResourceIdentify, IActionDerivedResourceIdentify, IActorDerivedResourceIdentifyOutput, IActorDerivedResourceIdentifyArgs } from '@comunica/bus-derived-resource-identify';
import { KeysDerivedResourceIdentify } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, passTestVoid, failTest } from '@comunica/core';
import { ComunicaDataFactory } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';

/**
 * A comunica Qpf Derived Resource Identify Actor.
 */
export class ActorDerivedResourceIdentifyQpf extends ActorDerivedResourceIdentify {
  protected dataFactory: ComunicaDataFactory = new DataFactory();
  protected algebraFactory: AlgebraFactory = new AlgebraFactory(this.dataFactory);

  public constructor(args: IActorDerivedResourceIdentifyArgs) {
    super(args);
  }

  public async test(action: IActionDerivedResourceIdentify): Promise<TestResult<IActorTest>> {
    if (action.derivedResourceUnidentified.filter !== 'qpf'){
      return failTest(`${this.name} can only identify qpf derived resources`);
    }
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourceIdentify): Promise<IActorDerivedResourceIdentifyOutput> {
    const derivedResource: IActorDerivedResourceIdentifyOutput = {
      derivedResourceIdentified: {
        ...action.derivedResourceUnidentified,
        selectorShape: {
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
        },
        resourceCoefficients:  {
          selecitivty: 1,
          requests: 10,
          compute: 1
        }        
      }
    }

    return derivedResource;
  }
}
