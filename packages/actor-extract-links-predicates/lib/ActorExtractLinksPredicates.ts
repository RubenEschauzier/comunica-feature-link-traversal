import type { IActionExtractLinks, IActorExtractLinksArgs, IActorExtractLinksOutput } from '@comunica/bus-extract-links';
import { ActorExtractLinks } from '@comunica/bus-extract-links';
import type { IActorArgs, IActorTest } from '@comunica/core';

/**
 * A comunica Traverse Predicates RDF Metadata Extract Actor.
 */
export class ActorExtractLinksPredicates extends ActorExtractLinks {
  private readonly checkSubject: boolean;
  private readonly predicates: RegExp[];
  private readonly predicatesList: string[]

  public constructor(args: IActorExtractLinksTraversePredicatesArgs) {
    super(args);

    this.predicates = args.predicateRegexes.map(stringRegex => new RegExp(stringRegex, 'u'));
    this.predicatesList = args.predicateRegexes;
  }

  public async test(action: IActionExtractLinks): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionExtractLinks): Promise<IActorExtractLinksOutput> {
    const links =  await ActorExtractLinks.collectStream(action.metadata, (quad, links) => {
      if (!this.checkSubject || this.subjectMatches(quad.subject.value, action.url)) {
        for (const regex of this.predicates) {
          if (regex.test(quad.predicate.value)) {
            links.push({ url: quad.object.value });
            break;
          }
        }
      }
    });
    // If we found links we add to the traversed graph
    if (links.length > 0){
      const metaData: Record<string, any>[] = []
      for (let i = 0; i<links.length; i++){
        metaData.push({linkSource: 'ExtractLinkPredicates', dereferenced: false, predicates: this.predicatesList})
      }
      this.addLinksToGraph(action.url, links, metaData, action.context, false);
    }

    return {
      links: links
    };
  }

  private subjectMatches(subject: string, url: string): boolean {
    const fragmentPos = subject.indexOf('#');
    if (fragmentPos >= 0) {
      subject = subject.slice(0, fragmentPos);
    }
    return subject === url;
  }
}

export interface IActorExtractLinksTraversePredicatesArgs
  extends IActorExtractLinksArgs {
  /**
   * If only quads will be considered that have a subject equal to the request URL.
   */
  checkSubject: boolean;
  /**
   * A list of regular expressions that will be tested against predicates of quads.
   */
  predicateRegexes: string[];
}
