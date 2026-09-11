import { IDerivedResource } from '@comunica/actor-extract-links-solid-derived-resources';
import { ActorDerivedResourcePropose, IActionDerivedResourcePropose, IActorDerivedResourceProposeOutput, IActorDerivedResourceProposeArgs, ICandidateResource } from '@comunica/bus-derived-resource-propose';
import { TestResult, IActorTest, passTestVoid } from '@comunica/core';
import { Algebra, AlgebraFactory, algebraUtils } from '@comunica/utils-algebra';
import { canAnswerBgp } from '@comunica/utils-query-operation';

/**
 * A comunica Stars Derived Resource Propose Actor. This extracts any sub-queries in
 * star-shape and returns them
 */
export class ActorDerivedResourceProposeStars extends ActorDerivedResourcePropose {
  protected readonly algebraFactory = new AlgebraFactory();

  public constructor(args: IActorDerivedResourceProposeArgs) {
    super(args);
  }

  public async test(action: IActionDerivedResourcePropose): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionDerivedResourcePropose): Promise<IActorDerivedResourceProposeOutput> {
    const bgps: Algebra.Bgp[] = [];
    algebraUtils.visitOperation(action.operation, {
      [Algebra.Types.BGP]: {
        preVisitor: () => ({ continue: false }),
        visitor: (pattern) => {
          bgps.push(pattern)
        },
      },
    });
    const proposedStarResources: ICandidateResource[] = [];
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
        for (const resource of action.resources) {
          const selectorShape = await resource.querySource.getSelectorShape(action.context);
          if (selectorShape.type !== 'operation'){
            continue;
          }
          if (canAnswerBgp(selectorShape, this.algebraFactory.createBgp(starPatterns),
            selectorShape.variablesOptional ?? [], selectorShape.variablesRequired ?? [])
          ){
            proposedStarResources.push(
              {
                operations: starPatterns,
                resource,
                kind: "star",
                anchorTerms: [ starPatterns[0].subject ],
                prunable: true,
                features: {
                  patternCount: starPatterns.length,
                  constantCount: starPatterns[0].subject.termType !== "Variable" ? 1 : 0 + 
                    starPatterns.reduce(
                      (acc: number, pattern) => acc + pattern.object.termType !== 'Variable' ? 1 : 0, 0
                    ),
                  coefficients: {
                    compute: 5,
                    requests: 1,
                    selectivity: 5
                  }
                 }
              }
            )
          }
        }
      }
    }

    return {
      candidateResources: proposedStarResources
    }
  }
}
