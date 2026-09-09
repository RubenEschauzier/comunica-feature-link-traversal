import { IDerivedResource, IDerivedResourceCoefficients } from '@comunica/actor-extract-links-solid-derived-resources';
import { ActorDerivedResourceSelect, IActionDerivedResourceSelect, IActorDerivedResourceSelectOutput, IActorDerivedResourceSelectArgs, IActorDerivedResourceSelectTestSideData, IRequiredResources } from '@comunica/bus-derived-resource-select';
import { TestResult, IActorTest, failTest, passTestWithSideData, ActionContext } from '@comunica/core';
import type { MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import { Bindings, ComunicaDataFactory, FragmentSelectorShape } from '@comunica/types';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { canAnswerBgp } from '@comunica/utils-query-operation';
import { KeysDerivedResourceSelect, KeysQuerySourceIdentifyLinkTraversal } from '@comunica/context-entries-link-traversal';
import { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import { KeysInitQuery, KeysRdfJoin } from '@comunica/context-entries';
import type * as RDF from '@rdfjs/types';
import { extractLinearSubqueries } from './LinearSubqueries';

/**
 * A comunica Linear Query Derived Resource Select Actor.
 */
export class ActorDerivedResourceSelectLinearQuery extends 
ActorDerivedResourceSelect<IActorDerivedResourceSelectTestSideData> {
  protected dataFactory: ComunicaDataFactory = new DataFactory();
  protected algebraFactory: AlgebraFactory = new AlgebraFactory(this.dataFactory);
  
  public readonly mediatorMetadata: MediatorRdfMetadata;
  public readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;

  protected derivedResourceCoefficients: IDerivedResourceCoefficients;
  protected minChainLength: number;
  protected maxChainLength: number;

  public constructor(args: IActorDerivedResourceSelectLinearQueryArgs) {
    super(args);
    this.mediatorMetadata = args.mediatorMetadata;
    this.mediatorMetadataExtract = args.mediatorMetadataExtract;
    this.derivedResourceCoefficients = args.derivedResourceCoefficients;
    this.minChainLength = args.minChainLength;
    this.maxChainLength = args.maxChainLength;
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
    // TODO Use signal to abort dereference operation (is this needed for experiments?)
    const controller = new AbortController();
    const signal = controller.signal;
    
    // Ensure the link traversal manager knows we're dereferencing this derived
    // resource
    const context = action.context;
    const manager = context.getSafe(
      KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager
    );
    manager.addDereferencingDerivedResource(controller);

    const adaptiveJoinController = context.getSafe(KeysRdfJoin.adaptiveJoinController);

    const bgpsToResources = testResult.derivedResourceContext
      .getSafe(KeysDerivedResourceSelect.linearPatternToDerivedResource);
    
    // The chains are pattern-disjoint, see extractLinearSubqueries, so all of them can be sent
    // without two composite sources ever covering the same join entry
    await Promise.allSettled(
      Array.from(bgpsToResources.entries()).map(async ([patterns, resource]) => {
        // Any subject within the chain that isn't a variable must be within authoritativeness
        // of the source, otherwise discard
        const subjectTerms: RDF.Variable[] = [];
        for (let i = 0; i < patterns.length; i++) {
          const subjectTerm = patterns[i].subject;
          if (subjectTerm.termType !== 'Variable' && !subjectTerm.value.startsWith(resource.baseUrl)) {
            return; 
          }
          // Any variable bindings within the linear query must be checked for authoritativeness
          if (subjectTerm.termType === 'Variable'){
            subjectTerms.push(subjectTerm);
          }
        }

        let bindingsStream = resource.querySource.queryBindings(
          this.algebraFactory.createBgp(patterns),
          context,
        );

        // We filter any subject variable terms that are not within authority.
        // In chain ?s <p1> ?o1  ?o1 <p2> ?o2 we need to be authoritative over ?s and ?o1
        // not ?o2.
        if (subjectTerms.length > 0) {
          bindingsStream = bindingsStream.filter((binding: Bindings) => {
            const bound =  subjectTerms.map((subjTerm) => binding.get(subjTerm));
            return bound.every((term) => (term !== undefined && term.value.startsWith(resource.baseUrl)));
          });
        }

        // Extractors for checking authoritativeness on base operators
        // must be on both subject and object on inner triple patterns,
        // only outer pattern can have object outside of authoritativeness
        const patternFilterExtractors: RDF.Term[][] = [];
        for (let j = 0; j < patterns.length - 1; j++){
          patternFilterExtractors.push([ patterns[j].subject, patterns[j].object ]);
        }
        patternFilterExtractors.push([ patterns[patterns.length - 1].subject ])

        const added = adaptiveJoinController.addCompositeSource(patterns, bindingsStream, 
          {
            patternToExtractor: patternFilterExtractors,
            authoritativeDomain: resource.baseUrl,
          }
        );
        console.log(`Using composite linear source: ${added}`);

        // TODO Think about how reachability works when we aggregate over data.
        // When we aggregate over something that is not reachable, we will still include
        // it in results so reachability becomes muddy. Some formalizations maybe,
        // maybe call it the hybrid cMatch - all criterion?
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

    // The shapes are fetched once, as every chain below is matched against the same set
    const shapedResources = (await Promise.all(derivedResources.map(async resource => ({
      resource,
      shape: await resource.querySource.getSelectorShape(action.context),
    }))))
      .filter((shaped): shaped is IShapedResource => shaped.shape.type === 'operation');

    const usableResources = new Set<IDerivedResource>();
    const patternsToResource = new Map<Algebra.Pattern[], IDerivedResource>();
    for (const bgp of bgps) {
      for (const chain of extractLinearSubqueries(bgp.patterns)) {
        if (chain.length < this.minChainLength || chain.length > this.maxChainLength) {
          continue;
        }

        // A resource answers a path of exactly its own length, so the chain is only pushed down
        // when a resource matches it as a whole. We don't consider cutting up the chain if
        // a derived resource cannot answer the full chain
        const chainBgp = this.algebraFactory.createBgp(chain);
        const match = shapedResources.find(({ shape }) => canAnswerBgp(
          shape,
          chainBgp,
          shape.variablesOptional ?? [],
          shape.variablesRequired ?? [],
        ));
        if (!match) {
          continue;
        }

        usableResources.add(match.resource);
        patternsToResource.set(chain, match.resource);
      }
    }

    if (usableResources.size === 0){
      return { canAnswer: false, usableResources, derivedResourceContext: new ActionContext() }
    }
    return {
      canAnswer: true,
      usableResources,
      derivedResourceContext: new ActionContext().set(
        KeysDerivedResourceSelect.linearPatternToDerivedResource, patternsToResource
      )
    }
  }
}

/**
 * A derived resource together with its selector shape, narrowed to the operation shapes,
 * as those are the only shapes a BGP can be matched against.
 */
interface IShapedResource {
  resource: IDerivedResource;
  shape: Extract<FragmentSelectorShape, { type: 'operation' }>;
}

export interface IActorDerivedResourceSelectLinearQueryArgs
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
  /**
   * The shortest path that is pushed down to a derived resource.
   * @range {integer}
   * @default {2}
   */
  minChainLength: number;
  /**
   * The maximal linear query length to be pushed down to derived resource.
   * Note that linear lengths > 2 will not gain throughput increases, but might
   * decrease latency (arrival of first results).
   * @range {integer}
   * @default {2}
   */
  maxChainLength: number;
}