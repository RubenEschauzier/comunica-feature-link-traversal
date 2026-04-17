import type { IActionExtractLinks, IActorExtractLinksOutput } from '@comunica/bus-extract-links';
import { ActorExtractLinks } from '@comunica/bus-extract-links';
import type { IActorArgs, IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { IActionContext } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { Pattern } from '@comunica/utils-algebra/lib/Algebra';
import { DataFactory } from 'rdf-data-factory';
import { getNamedNodes, getTerms } from 'rdf-terms';

/**
 * A comunica Traverse All RDF Metadata Extract Actor.
 */
export class ActorExtractLinksAll extends ActorExtractLinks {
  public constructor(args: IActorArgs<IActionExtractLinks, IActorTest, IActorExtractLinksOutput>) {
    super(args);
  }

  public async test(_action: IActionExtractLinks): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionExtractLinks): Promise<IActorExtractLinksOutput> {
    return {
      links: await ActorExtractLinks.collectStream(action.metadata, (quad, links) => {
        for (const link of getNamedNodes(getTerms(quad))) {
          links.push({ url: link.value, metadata: { producedByActor: { name: this.name }}});
        }
      }),
    };
  }
  
  public getExtractPatternRepresentation(context: IActionContext): Pattern[]{
    const dataFactory = new DataFactory();
    const algebraFactory = new AlgebraFactory(dataFactory);

    return [ 
      algebraFactory.createPattern(
        dataFactory.variable('s'),
        dataFactory.variable('p'),
        dataFactory.variable('o'),
        dataFactory.variable('g'),
      )
    ]
  }
}
