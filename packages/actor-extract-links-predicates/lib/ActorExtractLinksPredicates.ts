import type { IActionExtractLinks, IActorExtractLinksOutput } from '@comunica/bus-extract-links';
import { ActorExtractLinks } from '@comunica/bus-extract-links';
import type { IActorArgs, IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { IActionContext } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { Pattern } from '@comunica/utils-algebra/lib/Algebra';
import { DataFactory } from 'rdf-data-factory';

/**
 * A comunica Traverse Predicates RDF Metadata Extract Actor.
 */
export class ActorExtractLinksPredicates extends ActorExtractLinks {
  private readonly checkSubject: boolean;
  private readonly predicates: RegExp[];
  private readonly stringPredicates: string[];

  public constructor(args: IActorExtractLinksTraversePredicatesArgs) {
    super(args);

    this.checkSubject = args.checkSubject;
    this.stringPredicates = args.predicateRegexes;
    this.predicates = args.predicateRegexes.map(stringRegex => new RegExp(stringRegex, 'u'));
  }

  public async test(_action: IActionExtractLinks): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionExtractLinks): Promise<IActorExtractLinksOutput> {
    return {
      links: await ActorExtractLinks.collectStream(action.metadata, (quad, links) => {
        if (!this.checkSubject || this.subjectMatches(quad.subject.value, action.url)) {
          for (const regex of this.predicates) {
            if (regex.test(quad.predicate.value)) {
              links.push({
                url: quad.object.value,
                metadata: {
                  producedByActor: {
                    name: this.name,
                    predicates: this.stringPredicates,
                    matchingPredicate: quad.predicate.value,
                    checkSubject: this.checkSubject,
                  },
                },
              });
              break;
            }
          }
        }
      }),
    };
  }

  private subjectMatches(subject: string, url: string): boolean {
    const fragmentPos = subject.indexOf('#');
    if (fragmentPos >= 0) {
      subject = subject.slice(0, fragmentPos);
    }
    return subject === url;
  }

  private evaluatePatterns(): Pattern[] {
    const dataFactory = new DataFactory();
    const algebraFactory = new AlgebraFactory(dataFactory);

    const specificPatterns: Pattern[] = [];

    for (const regexStr of this.stringPredicates) {
      // Remove exact match anchors (^ and $) and unescape characters
      const cleanedStr = regexStr.replace(/^\^|\$$/g, '').replace(/\\(.)/g, '$1');

      const containsRegexLogic = /[.*+?^${}()|[\]\\]/.test(cleanedStr);

      // If string is a complex regex, abort optimization and return the catch-all
      if (containsRegexLogic) {
        return [
          algebraFactory.createPattern(
            dataFactory.variable('s'),
            dataFactory.variable('p'),
            dataFactory.variable('o'),
            dataFactory.variable('g')
          )
        ];
      }

      // Otherwise, it is a simple string. Create a specific pattern.
      specificPatterns.push(
        algebraFactory.createPattern(
          dataFactory.variable('s'),
          dataFactory.namedNode(cleanedStr),
          dataFactory.variable('o'),
          dataFactory.variable('g')
        )
      );
    }

    return specificPatterns;
  }

  /**
   * If all regexes are simple strings, this actors requires only the passed
   * predicate regexes
   * @param context 
   * @returns 
   */
  public getExtractPatternRepresentation(context: IActionContext): Pattern[]{
    return this.evaluatePatterns();
  }
}

export interface IActorExtractLinksTraversePredicatesArgs
  extends IActorArgs<IActionExtractLinks, IActorTest, IActorExtractLinksOutput> {
  /**
   * If only quads will be considered that have a subject equal to the request URL.
   */
  checkSubject: boolean;
  /**
   * A list of regular expressions that will be tested against predicates of quads.
   */
  predicateRegexes: string[];
}
