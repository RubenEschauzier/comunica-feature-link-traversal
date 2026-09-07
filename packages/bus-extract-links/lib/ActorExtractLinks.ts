import type { IActorArgs, IActorOutput, IActorTest, Mediate, IAction } from '@comunica/core';
import { Actor } from '@comunica/core';
import type { IActionContext, ILink } from '@comunica/types';
import { Pattern } from '@comunica/utils-algebra/lib/Algebra';
import type * as RDF from '@rdfjs/types';

/**
 * A comunica actor for extract-links events.
 *
 * Actor types:
 * * Input:  IActionExtractLinks:      Metdata from which links can be extracted.
 * * Test:   <none>
 * * Output: IActorExtractLinksOutput: The extracted links.
 *
 * @see IActionExtractLinks
 * @see IActorExtractLinksOutput
 */
export abstract class ActorExtractLinks<TS = undefined>
  extends Actor<IActionExtractLinks, IActorTest, IActorExtractLinksOutput, TS> {
  /**
   * @param args - @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   */
  public constructor(args: IActorExtractLinksArgs<TS>) {
    super(args);
  }

  /**
   * The maximum number of listeners a metadata stream may have per event.
   * Every actor on the extract-links bus attaches its own listeners to the same metadata stream,
   * so the Node default of 10 is exceeded as soon as more than 10 extractors are configured,
   * which would emit a spurious MaxListenersExceededWarning.
   */
  public static readonly maxMetadataListeners = 64;

  /**
   * Raise the listener limit of a metadata stream that is shared by all actors on this bus,
   * so that legitimate fan-out over the bus does not trigger a memory leak warning.
   * @param metadata A metadata stream of quads.
   */
  public static allowSharedMetadataListeners(metadata: RDF.Stream): void {
    if (metadata.getMaxListeners() < ActorExtractLinks.maxMetadataListeners) {
      metadata.setMaxListeners(ActorExtractLinks.maxMetadataListeners);
    }
  }

  /**
   * A helper function to append links based on incoming quads.
   * @param metadata A metadata stream of quads.
   * @param onQuad A callback that will be invoked for each quad in the metadata stream.
   *               The second argument is the array of links that can be appended to.
   */
  public static collectStream(
    metadata: RDF.Stream,
    onQuad: (quad: RDF.Quad, links: ILink[]) => void,
  ): Promise<ILink[]> {
    ActorExtractLinks.allowSharedMetadataListeners(metadata);
    return new Promise((resolve, reject) => {
      const links: ILink[] = [];

      // Forward errors
      metadata.on('error', reject);

      // Invoke callback on each metadata quad
      metadata.on('data', (quad: RDF.Quad) => onQuad(quad, links));

      // Resolve to discovered links
      metadata.on('end', () => {
        resolve(links);
      });
    });
  }
  /**
   * Abstract getter that should return the quad patterns required
   * to get full coverage of quads containing extracted links.
   */
  public abstract getExtractPatternRepresentation(context: IActionContext): Pattern[];
}

export interface IActionExtractLinks extends IAction {
  /**
   * The page URL from which the quads were retrieved.
   */
  url: string;
  /**
   * The stream of quads to extract links from.
   */
  metadata: RDF.Stream;
  /**
   * The time it took to request the page in milliseconds.
   * This is the time until the first byte arrives.
   */
  requestTime: number;
  /**
   * The headers of the page.
   */
  headers?: Headers;
}

export interface IActorExtractLinksOutput extends IActorOutput {
  /**
   * The links to follow.
   */
  links: ILink[];
  /**
   * The conditional links.
   */
  linksConditional?: ILink[];
}

export type IActorExtractLinksArgs<TS = undefined> = IActorArgs<
IActionExtractLinks,
IActorTest,
IActorExtractLinksOutput,
TS
>;

export type MediatorExtractLinks = Mediate<
IActionExtractLinks,
IActorExtractLinksOutput
>;
