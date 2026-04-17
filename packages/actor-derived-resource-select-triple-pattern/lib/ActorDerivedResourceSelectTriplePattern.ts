import { IDerivedResource } from '@comunica/actor-extract-links-solid-derived-resources';
import { ActorDerivedResourceSelect, IActionDerivedResourceSelect, IActorDerivedResourceSelectOutput, IActorDerivedResourceSelectArgs, IActorDerivedResourceSelectTestSideData, IRequiredResources } from '@comunica/bus-derived-resource-select';
import { KeysInitQuery } from '@comunica/context-entries';
import { TestResult, IActorTest, failTest, passTest, passTestWithSideData } from '@comunica/core';
import { ComunicaDataFactory } from '@comunica/types';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { doesShapeAcceptOperation } from '@comunica/utils-query-operation';
import { KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';

/**
 * A comunica Triple Pattern Derived Resource Select Actor.
 */
export class ActorDerivedResourceSelectTriplePattern extends 
ActorDerivedResourceSelect<IActorDerivedResourceSelectTestSideData> {
  protected dataFactory: ComunicaDataFactory = new DataFactory();
  protected algebraFactory: AlgebraFactory = new AlgebraFactory(this.dataFactory);


  public constructor(args: IActorDerivedResourceSelectArgs) {
    super(args);
  }

  public async test(action: IActionDerivedResourceSelect): 
    Promise<TestResult<IActorTest, IActorDerivedResourceSelectTestSideData>> {
    const {canAnswer, usableResources } = this.hasRequiredResources(
      action.derivedResourcesIdentified, action
    );

    if (!canAnswer) {
      return failTest(`${this.name}: does not have the derived 
        resources required for the operation`);
    }

    return passTestWithSideData({}, 
      { usableResources: Array.from(usableResources.values()) }
    );
  }

  public async run(
    action: IActionDerivedResourceSelect,
    testResult: IActorDerivedResourceSelectTestSideData,
  ): Promise<IActorDerivedResourceSelectOutput> {
    // Abort controller indicating forcefull exit of traversal process.
    // this should also abort the dereferencing of the derived resource.
    // TODO Add function callback
    const abortController = new AbortController();

    const context = action.context;
    const manager = context.getSafe(
      KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager
    );
    manager.addDereferencingDerivedResource(abortController);

    const usableDerivedResources = testResult.usableResources;
    // Then we should add the reasoning on what resource to use to actually
    // do the operation this select actor wants to do. Maybe make this 
    // a generic reasoner or maybe make it derived resource specific.
    
    // Then after deciding the derived resource to use, execute the specific queries
    // execute them, extract metadata (just as in dereference-link-hypermedia)

    // Then update metadata of aggregatedStore, import the quads (only the ones in query)
    // and update the link queue to immediately filter URLs in link queue that match
    // the data source glob pattern.

    // After all is done, remove the abortController.

    return true;
  }

  public override hasRequiredResources(
    derivedResources: IDerivedResource[],
    action: IActionDerivedResourceSelect,
  ): IRequiredResources {
    const usableResources: Set<IDerivedResource> = new Set();
    const {patterns, paths} = this.extractTriplePatterns(action.context.getSafe(KeysInitQuery.query));
    if (paths.length > 0 ){
      // TODO: For path we need to convert the path to the required predicates to answer the traversal of the path
      // so we need to deconstruct the path pattern. (see claude response)
      // if qpf we can't do a certain path -> return false
      // After converting to triple patterns add it to the patterns array and continue with the canonicalForm
      // checks
    }
    const canonicalForms = this.triplePatternsToFragmentTest(patterns);
    for (const cForm of canonicalForms){
      let canAnswer = false;
      for (const derivedResource of derivedResources){
        if (doesShapeAcceptOperation(derivedResource.derivedResourceSelectorShape, cForm)){
          usableResources.add(derivedResource);
          canAnswer = true;
        }
      }
      if (!canAnswer){
        return {canAnswer: false, usableResources: new Set()};
      }
    }
    return {canAnswer: true, usableResources};

  }

  /**
   * Extract all triple patterns from a query algebra tree.
   */
  private extractTriplePatterns(operation: Algebra.Operation):  
    { patterns: Algebra.Pattern[], paths: Algebra.Path[] } {
    const patterns: Algebra.Pattern[] = [];
    const paths: Algebra.Path[] = [];

    algebraUtils.visitOperation(operation, {
      [Algebra.Types.BGP]: {
        visitor(bgp: Algebra.Bgp) {
          patterns.push(...bgp.patterns);
        },
      },
      [Algebra.Types.PATH]: {
        visitor(path: Algebra.Path) {
          paths.push(path);
        },
      },
    });

    return { patterns, paths };
  }

private triplePatternsToFragmentTest(patterns: Algebra.Pattern[]): Algebra.Pattern[] {
    const seenSignatures = new Set<string>();
    const canonicalPatterns: Algebra.Pattern[] = [];

    // Helper function to evaluate structure, prevent duplicates, and generate the pattern
    const addTestPattern = (
      isSubjVar: boolean, 
      isPredVar: boolean, 
      isObjVar: boolean, 
      isGraphVar: boolean
    ) => {
      const signature = `${isSubjVar}-${isPredVar}-${isObjVar}-${isGraphVar}`;
      
      if (!seenSignatures.has(signature)) {
        seenSignatures.add(signature);
        canonicalPatterns.push(
          this.algebraFactory.createPattern(
            isSubjVar ? this.dataFactory.variable('s') : this.dataFactory.namedNode('s'),
            isPredVar ? this.dataFactory.variable('p') : this.dataFactory.namedNode('p'),
            isObjVar ? this.dataFactory.variable('o') : this.dataFactory.namedNode('o'),
            isGraphVar ? this.dataFactory.variable('g') : this.dataFactory.namedNode('g')
          )
        );
      }
    };

    // Always test bound predicate as this is required for link extraction
    addTestPattern(true, false, true, true);

    for (const pattern of patterns) {
      addTestPattern(
        pattern.subject.termType === 'Variable',
        pattern.predicate.termType === 'Variable',
        pattern.object.termType === 'Variable',
        pattern.graph.termType === 'Variable'
      );
    }

    return canonicalPatterns;
  }
}

export interface IActorDerivedResourceSelectTriplePatternArgs 
extends IActorDerivedResourceSelectArgs {
  /**
   * The coefficients for choosing the best resource. By default
   * prefers least requests
   */
  derivedResourceCoefficients: {
    compute: 0
    requests: 1,
    selectivity: 0
  }
}