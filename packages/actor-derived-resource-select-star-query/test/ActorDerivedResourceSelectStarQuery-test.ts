import { Bus, ActionContext } from '@comunica/core';
import { DataFactory } from 'rdf-data-factory';
import { AlgebraFactory, Algebra } from '@comunica/utils-algebra';
import { KeysInitQuery } from '@comunica/context-entries';
import {
  KeysDerivedResourceSelect,
  KeysQuerySourceIdentifyLinkTraversal,
} from '@comunica/context-entries-link-traversal';
import { ActorDerivedResourceSelectStarQuery } from '../lib/ActorDerivedResourceSelectStarQuery';
import '@comunica/utils-jest';

const DF = new DataFactory();
const AF = new AlgebraFactory(DF);

describe('ActorDerivedResourceSelectStarQuery', () => {
  let bus: any;
  let mediatorMetadata: any;
  let mediatorMetadataExtract: any;
  let actor: ActorDerivedResourceSelectStarQuery;
  let linkTraversalManager: any;

  const VAR_S = DF.variable('s');
  const VAR_O1 = DF.variable('o1');
  const VAR_O2 = DF.variable('o2');
  const PRED1 = DF.namedNode('http://example.org/pred1');
  const PRED2 = DF.namedNode('http://example.org/pred2');

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    mediatorMetadata = { mediate: jest.fn() };
    mediatorMetadataExtract = { mediate: jest.fn() };
    actor = new ActorDerivedResourceSelectStarQuery({
      name: 'actor',
      bus,
      derivedResourceCoefficients: { compute: 1, requests: 1, selectivity: 1 },
      mediatorMetadata,
      mediatorMetadataExtract,
    });

    linkTraversalManager = {
      addDereferencingDerivedResource: jest.fn(),
      removeDereferencingDerivedResource: jest.fn(),
    };
  });

  describe('hasRequiredResources', () => {
    let mockResource: any;
    let queryBgp: Algebra.Bgp;
    let action: any;

    beforeEach(() => {
      const pattern1 = AF.createPattern(VAR_S, PRED1, VAR_O1);
      const pattern2 = AF.createPattern(VAR_S, PRED2, VAR_O2);
      queryBgp = AF.createBgp([pattern1, pattern2]);

      const context = new ActionContext({
        [KeysInitQuery.query.name]: queryBgp,
      });

      action = {
        context,
        derivedResourcesIdentified: [],
      };

      mockResource = {
        iri: 'http://example.org/derived',
        querySource: {
          getSelectorShape: jest.fn().mockResolvedValue({
            type: 'operation',
            operation: {
              operationType: 'pattern',
              pattern: AF.createBgp([
                AF.createPattern(VAR_S, DF.variable('p1'), VAR_O1),
                AF.createPattern(VAR_S, DF.variable('p2'), VAR_O2),
              ]),
            },
            variablesOptional: [DF.variable('p1'), DF.variable('p2')],
          }),
          queryQuads: jest.fn(),
        },
      };
    });

    it('should return canAnswer: false when no derived resources are provided', async () => {
      const result = await actor.hasRequiredResources([], action);
      expect(result.canAnswer).toBe(false);
      expect(result.usableResources.size).toBe(0);
    });

    it('should ignore patterns with non-NamedNode predicates', async () => {
      const varPredPattern = AF.createPattern(VAR_S, DF.variable('varPred'), VAR_O1);
      const nonConcreteBgp = AF.createBgp([varPredPattern]);
      action.context = action.context.set(KeysInitQuery.query, nonConcreteBgp);

      const result = await actor.hasRequiredResources([mockResource], action);
      expect(result.canAnswer).toBe(false);
      expect(mockResource.querySource.getSelectorShape).not.toHaveBeenCalled();
    });

    it('should skip resources with selectorShape type other than operation', async () => {
      mockResource.querySource.getSelectorShape.mockResolvedValue({
        type: 'composite',
      });

      const result = await actor.hasRequiredResources([mockResource], action);
      expect(result.canAnswer).toBe(false);
      expect(result.usableResources.size).toBe(0);
    });

    it('should return canAnswer: false when selectorShape cannot answer the star pattern', async () => {
      mockResource.querySource.getSelectorShape.mockResolvedValue({
        type: 'operation',
        operation: {
          operationType: 'pattern',
          // Only 1 pattern in shape, but query has 2
          pattern: AF.createBgp([
            AF.createPattern(VAR_S, DF.variable('p1'), VAR_O1),
          ]),
        },
        variablesOptional: [DF.variable('p1')],
      });

      const result = await actor.hasRequiredResources([mockResource], action);
      expect(result.canAnswer).toBe(false);
      expect(result.usableResources.size).toBe(0);
    });

    it('should return canAnswer: true and map patterns to resource when shape matches', async () => {
      const result = await actor.hasRequiredResources([mockResource], action);
      expect(result.canAnswer).toBe(true);
      expect(result.usableResources.has(mockResource)).toBe(true);

      const mapped = result.derivedResourceContext.get(
        KeysDerivedResourceSelect.starPatternToDerivedResource,
      );
      expect(mapped).toBeDefined();
      expect(mapped!.size).toBe(1);
      expect(Array.from(mapped!.values())[0]).toBe(mockResource);
    });

    it('should work when queryOperation is wrapped in Algebra.Project (SELECT query)', async () => {
      const projectOperation = AF.createProject(queryBgp, [VAR_S, VAR_O1, VAR_O2]);
      action.context = action.context.set(KeysInitQuery.query, projectOperation);

      const result = await actor.hasRequiredResources([mockResource], action);
      expect(result.canAnswer).toBe(true);
      expect(result.usableResources.has(mockResource)).toBe(true);
    });
  });

  describe('test', () => {
    let mockResource: any;
    let action: any;

    beforeEach(() => {
      const pattern1 = AF.createPattern(VAR_S, PRED1, VAR_O1);
      const pattern2 = AF.createPattern(VAR_S, PRED2, VAR_O2);
      const queryBgp = AF.createBgp([pattern1, pattern2]);

      const context = new ActionContext({
        [KeysInitQuery.query.name]: queryBgp,
      });

      mockResource = {
        iri: 'http://example.org/derived',
        querySource: {
          getSelectorShape: jest.fn().mockResolvedValue({
            type: 'operation',
            operation: {
              operationType: 'pattern',
              pattern: AF.createBgp([
                AF.createPattern(VAR_S, DF.variable('p1'), VAR_O1),
                AF.createPattern(VAR_S, DF.variable('p2'), VAR_O2),
              ]),
            },
            variablesOptional: [DF.variable('p1'), DF.variable('p2')],
          }),
        },
      };

      action = {
        context,
        derivedResourcesIdentified: [mockResource],
      };
    });

    it('should fail when hasRequiredResources returns canAnswer: false', async () => {
      action.derivedResourcesIdentified = [];
      const testPromise = actor.test(action);
      await expect(testPromise).resolves.toFailTest(
        `${actor.name}: does not have the derived \n        resources required for the operation`,
      );
    });

    it('should pass and return sideData when hasRequiredResources returns canAnswer: true', async () => {
      const result: any = await actor.test(action);
      expect(result).toMatchObject({
        sideData: {
          usableResources: [mockResource],
        },
      });
      expect(result.sideData?.derivedResourceContext.has(
        KeysDerivedResourceSelect.starPatternToDerivedResource,
      )).toBe(true);
    });
  });

  describe('run', () => {
    it('should manage linkTraversalManager lifecycle and query bindings on resources', async () => {
      const pattern1 = AF.createPattern(VAR_S, PRED1, VAR_O1);
      const pattern2 = AF.createPattern(VAR_S, PRED2, VAR_O2);
      const patterns = [pattern1, pattern2];

      const mockQuerySource = {
        queryBindings: jest.fn(),
      };
      const mockResource = {
        iri: 'http://example.org/derived',
        querySource: mockQuerySource,
      };

      const starPatternMap = new Map<Algebra.Pattern[], any>();
      starPatternMap.set(patterns, mockResource);

      const derivedResourceContext = new ActionContext({
        [KeysDerivedResourceSelect.starPatternToDerivedResource.name]: starPatternMap,
      });

      const context = new ActionContext({
        [KeysQuerySourceIdentifyLinkTraversal.linkTraversalManager.name]: linkTraversalManager,
      });

      const action: any = {
        context,
        derivedResourcesIdentified: [mockResource],
      };

      const testResult: any = {
        usableResources: [mockResource],
        derivedResourceContext,
      };

      const output = await actor.run(action, testResult);

      expect(linkTraversalManager.addDereferencingDerivedResource).toHaveBeenCalledTimes(1);
      expect(mockQuerySource.queryBindings).toHaveBeenCalledWith(
        AF.createBgp(patterns),
        context,
      );
      expect(linkTraversalManager.removeDereferencingDerivedResource).toHaveBeenCalledTimes(1);
      expect(output).toEqual({ links: [] });
    });
  });
});
