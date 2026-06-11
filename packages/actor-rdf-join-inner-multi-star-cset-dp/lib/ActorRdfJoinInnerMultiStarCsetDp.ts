import { ActorRdfJoin, IActionRdfJoin, IActorRdfJoinOutput, IActorRdfJoinArgs } from '@comunica/bus-rdf-join';
import { TestResult, IActorTest, passTestVoid } from '@comunica/core';

/**
 * A comunica Inner Multi Star Cset Dp RDF Join Actor.
 */
export class ActorRdfJoinInnerMultiStarCsetDp extends ActorRdfJoin {
  public constructor(args: IActorRdfJoinArgs) {
    super(args);
  }

  public async test(action: IActionRdfJoin): Promise<TestResult<IActorTest>> {
    // Test for global statistics presence
    // test if it is a star-sub join
    // pass otherwise and getJoinCoefficients very low.
    return passTestVoid(); // TODO implement
  }

  public async run(action: IActionRdfJoin): Promise<IActorRdfJoinOutput> {
    // TODO: Should get global summary then bind global summary + type of star
    // to the star cardinality estimation method.
    // Then run DP.
    // From output order set each cardinality and do binary join
    // return output.
    return true;
  }
}
