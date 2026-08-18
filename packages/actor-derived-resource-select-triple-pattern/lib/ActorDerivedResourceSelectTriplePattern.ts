import { IDerivedResource, IDerivedResourceCoefficients } from '@comunica/actor-extract-links-solid-derived-resources';
import { ActorDerivedResourceSelect, IActionDerivedResourceSelect, IActorDerivedResourceSelectOutput, IActorDerivedResourceSelectArgs, IActorDerivedResourceSelectTestSideData, IRequiredResources } from '@comunica/bus-derived-resource-select';
import { TestResult, IActorTest, failTest, passTest, passTestWithSideData, ActionContext } from '@comunica/core';
import type { IActorRdfMetadataOutput, MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import { ComunicaDataFactory, ILink } from '@comunica/types';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { doesShapeAcceptOperation } from '@comunica/utils-query-operation';
import { KeysDerivedResourceSelect, KeysQuerySourceIdentifyLinkTraversal, KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { ActorExtractLinks, MediatorExtractLinks } from '@comunica/bus-extract-links';
import { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';

/**
 * A comunica Triple Pattern Derived Resource Select Actor.
 */
export class ActorDerivedResourceSelectTriplePattern extends 
ActorDerivedResourceSelect<IActorDerivedResourceSelectTestSideData> {
  protected dataFactory: ComunicaDataFactory = new DataFactory();
  protected algebraFactory: AlgebraFactory = new AlgebraFactory(this.dataFactory);
  
  public readonly mediatorMetadata: MediatorRdfMetadata;
  public readonly mediatorExtractLinks: MediatorExtractLinks;
  public readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;

  protected derivedResourceCoefficients: IDerivedResourceCoefficients;

  public constructor(args: IActorDerivedResourceSelectTriplePatternArgs) {
    super(args);
    this.mediatorMetadata = args.mediatorMetadata;
    this.mediatorExtractLinks = args.mediatorExtractLinks;
    this.mediatorMetadataExtract = args.mediatorMetadataExtract;
    this.derivedResourceCoefficients = args.derivedResourceCoefficients;
  }

  public async test(action: IActionDerivedResourceSelect): 
    Promise<TestResult<IActorTest, IActorDerivedResourceSelectTestSideData>> {
    const {canAnswer, usableResources, derivedResourceContext } = 
      this.hasRequiredResources(action.derivedResourcesIdentified, action);

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

    const patternToResources = testResult.derivedResourceContext
      .getSafe(KeysDerivedResourceSelect.patternToDerivedResource);
    
    const bestResources = new Map(
      Array.from(patternToResources.entries(), ([pattern, resources]) => [
        pattern,
        resources
          .map(resource => ({
            resource,
            cost:
              resource.resourceCoefficients.compute * this.derivedResourceCoefficients.compute +
              resource.resourceCoefficients.requests * this.derivedResourceCoefficients.requests +
              resource.resourceCoefficients.selectivity * this.derivedResourceCoefficients.selectivity
          }))
          .reduce((min, curr) => (curr.cost < min.cost ? curr : min))
      ])
    );  

    const discoveredLinks: ILink[] = [];
    const discoveredLinksSet: Set<string> = new Set();

    await Promise.all(Array.from(bestResources.entries()).map(async ([pattern, bestResource]) => {
      if (signal.aborted){
        return;
        // TODO: Abort here
      }
      const rawQuads = bestResource.resource.querySource.queryQuads(
        pattern, context
      );
      // Filter the links matching the selector pattern of the resource.
      const dynamicLinkFilter = action.context.getSafe(KeysRdfResolveHypermediaLinks.dynamicFilter);
      const selectors = bestResource.resource.selectors
      selectors.forEach((selector) => {
        if (this.isGlob(selector)){
          dynamicLinkFilter.addGlob(selector);
        }
        else {
          dynamicLinkFilter.addExact(selector)
        }
      });

      // TODO: Think about how reachability works when we aggregate over data.
      // When we aggregate over something that is not reachable, we will still include
      // it in results so reachability becomes muddy. Some formalizations maybe,
      // maybe call it the hybrid cMatch - all criterion?
      const rdfMetadataOutput: IActorRdfMetadataOutput = await this.mediatorMetadata.mediate(
        { context, url: bestResource.resource.iri, quads: rawQuads },
      );
      const { links } = await this.mediatorExtractLinks.mediate({
        context,
        url: bestResource.resource.iri,
        metadata: rdfMetadataOutput.metadata, 
        requestTime: 0,
      });
      for (const link of links){
        if (!discoveredLinksSet.has(link.url)){
          discoveredLinks.push(link);
          discoveredLinksSet.add(link.url);
        }
      }
      
      // TODO add back abort functionality
      const eventEmitter = manager.getAggregatedStore().import(rdfMetadataOutput.data);
      await new Promise<void>((resolve, reject) => {
        // When traversal is aborted, we destroy the stream and
        // exit all dereference logic of this function.
        // const onAbort = () => {
        //   // TODO: How else do I do this?
        //   (<any>rdfMetadataOutput.data).destroy(new Error('Traversal aborted'));
        //   reject(new Error('Aborted during stream processing'));
        // };
        // signal.addEventListener('abort', onAbort);
        eventEmitter.on('end', () => {
          // signal.removeEventListener('abort', onAbort);
          resolve();
        });

        eventEmitter.on('error', (err) => {
          // signal.removeEventListener('abort', onAbort);
          reject(err);
        });
      });
    }));
    
    // Done dereferencing, so remove controller
    manager.removeDereferencingDerivedResource(controller);
    return { links: discoveredLinks };
  }

  public override hasRequiredResources(
    derivedResources: IDerivedResource[],
    action: IActionDerivedResourceSelect,
  ): IRequiredResources {
    const actorsExtractLink = <ActorExtractLinks[]>((<any>this.mediatorExtractLinks.bus).actors);

    // Get unique patterns required to do traversal
    const seen = new Set<string>();
    const patterns = actorsExtractLink
      .flatMap(actor => actor.getExtractPatternRepresentation(action.context))
      .filter(pattern => {
        // Generate a unique signature for the pattern's shape
        const key = [pattern.subject, pattern.predicate, pattern.object, pattern.graph]
          .map(term => term.termType === 'Variable' ? 'VAR' : term.value)
          .join('|');

        // Keep only the first instance of each unique signature
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });   

    const usableResources: Set<IDerivedResource> = new Set();
    const patternToResources: Map<Algebra.Pattern, IDerivedResource[]> = new Map();

    const derivedResourceContext = new ActionContext()
    .set(KeysDerivedResourceSelect.patternToDerivedResource, patternToResources);

    for (const pattern of patterns){
      let canAnswer = false;
      for (const derivedResource of derivedResources){
        if (doesShapeAcceptOperation(derivedResource.derivedResourceSelectorShape, pattern)){
          usableResources.add(derivedResource);
          if (!patternToResources.has(pattern)){
            patternToResources.set(pattern, []);
          }
          patternToResources.get(pattern)!.push(derivedResource);
          canAnswer = true;
        }
      }
      if (!canAnswer){
        return {
          canAnswer: false, 
          usableResources: new Set(), 
          derivedResourceContext: new ActionContext()
        };
      }
    }
    return {canAnswer: true, usableResources, derivedResourceContext };
  }
}

export interface IActorDerivedResourceSelectTriplePatternArgs 
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
   * Extract links mediator, used to determine the required triple 
   * pattern queries to extract all links for traversal.
   */
  mediatorExtractLinks: MediatorExtractLinks;
  /**
   * The metadata extract mediator
  */
  mediatorMetadataExtract: MediatorRdfMetadataExtract;

}