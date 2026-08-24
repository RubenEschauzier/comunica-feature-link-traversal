import { IDerivedResource, IDerivedResourceCoefficients } from '@comunica/actor-extract-links-solid-derived-resources';
import { ActorDerivedResourceSelect, IActionDerivedResourceSelect, IActorDerivedResourceSelectOutput, IActorDerivedResourceSelectArgs, IActorDerivedResourceSelectTestSideData, IRequiredResources } from '@comunica/bus-derived-resource-select';
import { TestResult, IActorTest, failTest, passTestWithSideData, ActionContext } from '@comunica/core';
import type { IActorRdfMetadataOutput, MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import { ComunicaDataFactory, ILink } from '@comunica/types';
import { Algebra, AlgebraFactory } from '@comunica/utils-algebra';
import { DataFactory } from 'rdf-data-factory';
import { doesShapeAcceptOperation } from '@comunica/utils-query-operation';
import { KeysDerivedResourceSelect, KeysQuerySourceIdentifyLinkTraversal, KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import { ActorExtractLinks, MediatorExtractLinks } from '@comunica/bus-extract-links';
import { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import type * as RDF from '@rdfjs/types';

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
    const { canAnswer, usableResources, derivedResourceContext } =
      await this.hasRequiredResources(action.derivedResourcesIdentified, action);
    if (!canAnswer) {
      return failTest(`${this.name}: does not have the derived resources required for the operation`);
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

    // Pick lowest-cost resource per pattern
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

    const dynamicLinkFilter = action.context.getSafe(KeysRdfResolveHypermediaLinks.dynamicFilter);
    const discoveredLinks: ILink[] = [];
    const discoveredLinksSet: Set<string> = new Set();

    // 2. Register all selectors upfront synchronously so traversal engine does not crawl them
    for (const [, bestResource] of bestResources.entries()) {
      for (const selector of bestResource.resource.selectors) {
        if (this.isGlob(selector)) {
          dynamicLinkFilter.addGlob(selector);
        } else {
          dynamicLinkFilter.addExact(selector);
        }
      }
    }

    // 3. Dispatch all resource streams in parallel (Eliminates sequential mediator & fetch roundtrips)
    const resourceTasks = Array.from(bestResources.entries()).map(async ([pattern, bestResource]) => {
      if (signal.aborted) return;

      const rawQuads = bestResource.resource.querySource.queryQuads(pattern, context);
      signal.addEventListener('abort', () => rawQuads.destroy(new Error('Traversal aborted')), { once: true });

      const rdfMetadataOutput: IActorRdfMetadataOutput = await this.mediatorMetadata.mediate(
        { context, url: bestResource.resource.iri, quads: rawQuads },
      );

      // Ingestion and link extraction run concurrently on the split streams
      const importPromise = (async () => {
        const eventEmitter = manager.getAggregatedStore().import(rdfMetadataOutput.data);
        await this.waitForImport(eventEmitter, rdfMetadataOutput.data, signal);
      })();

      const extractPromise = this.mediatorExtractLinks.mediate({
        context,
        url: bestResource.resource.iri,
        metadata: rdfMetadataOutput.metadata,
        requestTime: 0,
      })
      // .then(({ links }) => {
      //   for (const link of links) {
      //     if (!discoveredLinksSet.has(link.url)) {
      //       discoveredLinksSet.add(link.url);
      //       discoveredLinks.push(link);
      //       if (manager.pushLink) {
      //         manager.pushLink(link);
      //       }
      //     }
      //   }
      // });

      await Promise.all([extractPromise]);
    });

    // 4. Concurrently settle the background imports without blocking returning discovered links
    void Promise.allSettled(resourceTasks).then((results) => {
      const failed = results.find(
        (result): result is PromiseRejectedResult => result.status === 'rejected',
      );
      manager.completeDereferencingDerivedResource(
        controller,
        failed ? this.toError(failed.reason) : undefined,
      );
    });

    return { links: discoveredLinks };
  }

  private toError(error: unknown): Error {
    return error instanceof Error ? error : new Error(String(error));
  }

  private waitForImport(
    eventEmitter: NodeJS.EventEmitter,
    data: RDF.Stream,
    signal: AbortSignal,
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const onAbort = (): void => {
        (<any>data).destroy(new Error('Traversal aborted'));
      };
      const onEnd = (): void => {
        signal.removeEventListener('abort', onAbort);
        resolve();
      };
      const onError = (error: Error): void => {
        signal.removeEventListener('abort', onAbort);
        if (signal.aborted) {
          resolve();
        } else {
          reject(error);
        }
      };

      eventEmitter.once('end', onEnd);
      eventEmitter.once('error', onError);
      signal.addEventListener('abort', onAbort, { once: true });

      if (signal.aborted) {
        onAbort();
      }
    });
  }

  public override async hasRequiredResources(
    derivedResources: IDerivedResource[],
    action: IActionDerivedResourceSelect,
  ): Promise<IRequiredResources> {
    const actorsExtractLink = <ActorExtractLinks[]>((<any>this.mediatorExtractLinks.bus).actors);

    const seen = new Set<string>();
    const patterns = actorsExtractLink
      .flatMap(actor => actor.getExtractPatternRepresentation(action.context))
      .filter(pattern => {
        const key = [pattern.subject, pattern.predicate, pattern.object, pattern.graph]
          .map(term => term.termType === 'Variable' ? 'VAR' : term.value)
          .join('|');

        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });

    const usableResources: Set<IDerivedResource> = new Set();
    const patternToResources: Map<Algebra.Pattern, IDerivedResource[]> = new Map();

    const derivedResourceContext = new ActionContext()
      .set(KeysDerivedResourceSelect.patternToDerivedResource, patternToResources);

    for (const pattern of patterns) {
      let canAnswer = false;
      for (const derivedResource of derivedResources) {
        if (doesShapeAcceptOperation(derivedResource.derivedResourceSelectorShape, pattern)) {
          
          usableResources.add(derivedResource);
          if (!patternToResources.has(pattern)) {
            patternToResources.set(pattern, []);
          }
          patternToResources.get(pattern)!.push(derivedResource);
          canAnswer = true;
        }
      }
      if (!canAnswer) {
        return {
          canAnswer: false,
          usableResources: new Set(),
          derivedResourceContext: new ActionContext()
        };
      }
    }
    return { canAnswer: true, usableResources, derivedResourceContext };
  }
}

export interface IActorDerivedResourceSelectTriplePatternArgs
  extends IActorDerivedResourceSelectArgs {
  derivedResourceCoefficients: IDerivedResourceCoefficients;
  mediatorMetadata: MediatorRdfMetadata;
  mediatorExtractLinks: MediatorExtractLinks;
  mediatorMetadataExtract: MediatorRdfMetadataExtract;
}