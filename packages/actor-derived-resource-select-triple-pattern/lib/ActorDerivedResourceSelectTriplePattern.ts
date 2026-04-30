import { IDerivedResource, IDerivedResourceCoefficients } from '@comunica/actor-extract-links-solid-derived-resources';
import { ActorDerivedResourceSelect, IActionDerivedResourceSelect, IActorDerivedResourceSelectOutput, IActorDerivedResourceSelectArgs, IActorDerivedResourceSelectTestSideData, IRequiredResources } from '@comunica/bus-derived-resource-select';
import { KeysInitQuery } from '@comunica/context-entries';
import { TestResult, IActorTest, failTest, passTest, passTestWithSideData, ActionContext } from '@comunica/core';
import { ComunicaDataFactory } from '@comunica/types';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { doesShapeAcceptOperation } from '@comunica/utils-query-operation';
import { KeysDerivedResourceSelect, KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import { ActorExtractLinks, MediatorExtractLinks } from '@comunica/bus-extract-links';
import { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';

/**
 * A comunica Triple Pattern Derived Resource Select Actor.
 */
export class ActorDerivedResourceSelectTriplePattern extends 
ActorDerivedResourceSelect<IActorDerivedResourceSelectTestSideData> {
  protected dataFactory: ComunicaDataFactory = new DataFactory();
  protected algebraFactory: AlgebraFactory = new AlgebraFactory(this.dataFactory);

  public readonly mediatorExtractLinks: MediatorExtractLinks;
  public readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;

  protected derivedResourceCoefficients: IDerivedResourceCoefficients;

  public constructor(args: IActorDerivedResourceSelectTriplePatternArgs) {
    super(args);
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
    // Abort controller indicating forcefull exit of traversal process.
    // this should also abort the dereferencing of the derived resource.
    // TODO Add function callback
    const abortController = new AbortController();

    const context = action.context;
    const manager = context.getSafe(
      KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager
    );
    manager.addDereferencingDerivedResource(abortController);

    const patternToResources = testResult.derivedResourceContext
      .getSafe(KeysDerivedResourceSelect.patternToDerivedResource);
    const bestResources = new Map(
      Array.from(patternToResources.entries(), ([pattern, resources]) => [
        pattern,
        resources
          .map(resource => {console.log(resource.resourceCoefficients); return ({
            resource,
            cost:
              resource.resourceCoefficients.compute * this.derivedResourceCoefficients.compute +
              resource.resourceCoefficients.requests * this.derivedResourceCoefficients.requests +
              resource.resourceCoefficients.selectivity * this.derivedResourceCoefficients.selectivity
          })})
          .reduce((min, curr) => (curr.cost < min.cost ? curr : min))
      ])
    );  
    for (const [pattern, bestResource] of bestResources.entries()){
      const quads = bestResource.resource.querySource.queryQuads(
        pattern, context
      );
      // TODO: Extract metadata
      // Update metadata of aggStore
      // import quads (should be only ones in query but this will do for now)
      // update link queue to filter URLs that match data source glob pattern


      const metadata = (await this.mediatorMetadataExtract.mediate({
        context,
        url: bestResource.resource.iri,
        // The problem appears to be conflicting metadata keys here
        metadata: quads,
        headers: new Headers(),
        requestTime: 0,
      })).metadata;
      // quads = rdfMetadataOutput.data;
      // manager.getQuerySourceAggregated().setBaseMetadata(metadata, this.aggregatedStore.containedSources.size > 0);
      // await this.aggregatedStore.importSource(nextLink.url, source, this.context);

    }
    
    // Done dereferencing, so remove controller
    manager.removeDereferencingDerivedResource(abortController);
    return true;
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
   * The coefficients for choosing the best resource.
   * It could be interesting to make these adaptive, for example,
   * when using QPF with many IRIs, such as <ex:s> <ex:p> ?o ? g
   * we can reasonably expect that QPF will require very little requests,
   * while if we use an ?s ?p ?o ?g pattern it will require more.
   */
  derivedResourceCoefficients: IDerivedResourceCoefficients
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