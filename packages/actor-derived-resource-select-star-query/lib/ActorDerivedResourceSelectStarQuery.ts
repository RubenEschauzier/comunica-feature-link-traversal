import { IDerivedResource, IDerivedResourceCoefficients } from '@comunica/actor-extract-links-solid-derived-resources';
import { ActorDerivedResourceSelect, IActionDerivedResourceSelect, IActorDerivedResourceSelectOutput, IActorDerivedResourceSelectArgs, IActorDerivedResourceSelectTestSideData, IRequiredResources } from '@comunica/bus-derived-resource-select';
import { TestResult, IActorTest, failTest, passTest, passTestWithSideData, ActionContext } from '@comunica/core';
import type { IActorRdfMetadataOutput, MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import { ComunicaDataFactory } from '@comunica/types';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { canAnswerBgp } from '@comunica/utils-query-operation';
import { KeysDerivedResourceSelect, KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import type * as RDF from '@rdfjs/types';
import { AsyncIterator } from 'asynciterator';
import { KeysInitQuery } from '@comunica/context-entries';

/**
 * A comunica Star Query Derived Resource Select Actor.
 */
export class ActorDerivedResourceSelectStarQuery extends 
ActorDerivedResourceSelect<IActorDerivedResourceSelectTestSideData> {
  protected dataFactory: ComunicaDataFactory = new DataFactory();
  protected algebraFactory: AlgebraFactory = new AlgebraFactory(this.dataFactory);
  
  public readonly mediatorMetadata: MediatorRdfMetadata;
  public readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;

  protected derivedResourceCoefficients: IDerivedResourceCoefficients;

  public constructor(args: IActorDerivedResourceSelectStarQueryArgs) {
    super(args);
    this.mediatorMetadata = args.mediatorMetadata;
    this.mediatorMetadataExtract = args.mediatorMetadataExtract;
    this.derivedResourceCoefficients = args.derivedResourceCoefficients;
  }

  public async test(action: IActionDerivedResourceSelect): 
    Promise<TestResult<IActorTest, IActorDerivedResourceSelectTestSideData>> {
    const {canAnswer, usableResources, derivedResourceContext } = 
      await this.hasRequiredResources(action.derivedResourcesIdentified, action);

      if (!canAnswer) {
      return failTest(`${this.name}: does not have the derived 
        resources required for the operation`);
    }

    return passTestWithSideData({}, 
      { usableResources: Array.from(usableResources.values()), derivedResourceContext }
    );
  }

  public async run(
    action: IActionDerivedResourceSelect,
    testResult: IActorDerivedResourceSelectTestSideData,
  ): Promise<IActorDerivedResourceSelectOutput> {
    const controller = new AbortController();
    const signal = controller.signal;
    
    const context = action.context;
    const manager = context.getSafe(
      KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager
    );
    manager.addDereferencingDerivedResource(controller);

    const bgpsToResources = testResult.derivedResourceContext
      .getSafe(KeysDerivedResourceSelect.starPatternToDerivedResource);
    
    await Promise.allSettled(
      Array.from(bgpsToResources.entries()).map(async ([patterns, resource]) => {
        const rawQuads = resource.querySource.queryQuads(
          this.algebraFactory.createBgp(patterns),
          context,
        );

        // TODO: Think about how reachability works when we aggregate over data.
        // When we aggregate over something that is not reachable, we will still include
        // it in results so reachability becomes muddy. Some formalizations maybe,
        // maybe call it the hybrid cMatch - all criterion?

        // TODO: Now we have to integrate it into our query plan!
      }),
    );
    
    manager.removeDereferencingDerivedResource(controller);
    return { links: [] };
  }

  public override async hasRequiredResources(
    derivedResources: IDerivedResource[],
    action: IActionDerivedResourceSelect,
  ): Promise<IRequiredResources> {
    const queryOperation = action.context.getSafe(KeysInitQuery.query);

    const bgps: Algebra.Bgp[] = [];
    algebraUtils.visitOperation(queryOperation, {
      [Algebra.Types.BGP]: {
        preVisitor: () => ({ continue: false }),
        visitor: (pattern) => {
          bgps.push(pattern)
        },
      },
    });

    const usableResources = new Set<IDerivedResource>();
    const patternsToResource = new Map<Algebra.Pattern[], IDerivedResource>();
    for (const bgp of bgps) {
      // Group patterns by subject (serialised by termType:value)
      const subStarsBySubject = new Map<string, Algebra.Pattern[]>();

      for (const pattern of bgp.patterns) {
        // Reject patterns where predicate is not a concrete IRI (no paths or unbound variables)
        if (pattern.predicate.termType !== 'NamedNode') {
          continue;
        }

        const subjectKey = `${pattern.subject.termType}:${pattern.subject.value}`;
        let group = subStarsBySubject.get(subjectKey);
        if (!group) {
          group = [];
          subStarsBySubject.set(subjectKey, group);
        }
        group.push(pattern);
      }

      // For each sub-star, match against derived resources
      for (const starPatterns of subStarsBySubject.values()) {
        for (const resource of derivedResources) {
          const selectorShape = await resource.querySource.getSelectorShape(action.context);
          if (selectorShape.type !== 'operation'){
            continue;
          }
          if (canAnswerBgp(selectorShape, this.algebraFactory.createBgp(starPatterns),
            selectorShape.variablesOptional ?? [], selectorShape.variablesRequired ?? [])
          ){
            usableResources.add(resource);
            patternsToResource.set(starPatterns, resource);
          }
        }
      }
    }
    if (usableResources.size === 0){
      return { canAnswer: false, usableResources, derivedResourceContext: new ActionContext() }
    }
    return {
      canAnswer: true,
      usableResources,
      derivedResourceContext: new ActionContext().set(
        KeysDerivedResourceSelect.starPatternToDerivedResource, patternsToResource
      )
    }
  }
}

export interface IActorDerivedResourceSelectStarQueryArgs 
extends IActorDerivedResourceSelectArgs {
  /**
   * The coefficients for choosing the best resource.
   * It could be interesting to make these adaptive, for example,
   * when using QPF with many IRIs, such as <ex:s> <ex:p> ?o ? g
   * we can reasonably expect that QPF will require very little requests,
   * while if we use an ?s ?p ?o ?g pattern it will require more.
   */
  derivedResourceCoefficients: IDerivedResourceCoefficients;
  /**
   * The metadata mediator
   */
  mediatorMetadata: MediatorRdfMetadata;
  /**
   * The metadata extract mediator
  */
  mediatorMetadataExtract: MediatorRdfMetadataExtract;

}