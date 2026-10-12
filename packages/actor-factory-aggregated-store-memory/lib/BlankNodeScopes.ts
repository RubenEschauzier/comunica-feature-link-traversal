import type { ComunicaDataFactory } from '@comunica/types';
import type * as RDF from '@rdfjs/types';

/**
 * The prefix a parser puts in front of every blank node label of a document, so that the blank nodes
 * of different documents stay apart. N3.js uses `b<counter>_`, a new counter for every parse.
 */
const PARSE_PREFIX = /^b\d+_/u;

/**
 * Gives a blank node the same term in every document and derived resource of a scope.
 *
 * Blank node labels only identify a node within one document, so every parse gives its blank nodes
 * labels of their own. A derived resource however answers over all documents of the pod it lives on,
 * which is only meaningful when the pod uses each label for one node throughout. Within such a scope,
 * the label the server stores is therefore taken to name the same node wherever it appears, and the
 * blank node gets the term of the first source it was seen in.
 *
 * A document can be read before the derived resources of its pod are known, so it is not known yet
 * which scope it is in. The terms it hands out are recorded under the document itself, and are found
 * again once the scope is declared, as the document lies within it.
 */
export class BlankNodeScopes {
  protected readonly dataFactory: ComunicaDataFactory;
  protected readonly scopes = new Set<string>();
  /**
   * The terms handed out per stored label, each with the source it was first seen in.
   */
  protected readonly terms = new Map<string, { source: string; term: RDF.BlankNode }[]>();
  protected counter = 0;

  public constructor(dataFactory: ComunicaDataFactory) {
    this.dataFactory = dataFactory;
  }

  /**
   * Declares that the documents under the given url and the derived resources answering over them
   * share their blank node labels.
   */
  public addScope(scope: string): void {
    this.scopes.add(scope);
  }

  /**
   * The scope the given url lies in, if any. The innermost scope wins, as found by walking up the
   * path of the url.
   */
  public scopeOf(url: string): string | undefined {
    if (this.scopes.size === 0) {
      return undefined;
    }
    let candidate = url.split(/[#?]/u)[0];
    while (candidate.length > 0) {
      if (this.scopes.has(candidate)) {
        return candidate;
      }
      const cut = candidate.lastIndexOf('/', candidate.endsWith('/') ? candidate.length - 2 : candidate.length - 1);
      if (cut < 0 || candidate.slice(0, cut + 1).endsWith('//')) {
        return undefined;
      }
      candidate = candidate.slice(0, cut + 1);
    }
    return undefined;
  }

  /**
   * The term for a blank node as a parser handed it out, for a document or derived resource read from
   * the given source. The parse prefix is dropped to get back the label the server stores.
   * @param term The blank node as parsed.
   * @param source The url the data was read from.
   * @param scope The scope of that url, as returned by scopeOf.
   */
  public forParsed(term: RDF.BlankNode, source: string, scope: string | undefined): RDF.BlankNode {
    return this.lookup(term.value.replace(PARSE_PREFIX, ''), source, scope, term);
  }

  /**
   * The term for a blank node carrying the label the server stores, as a SPARQL result does.
   * @param label The label as stored.
   * @param source The url the data was read from.
   * @param scope The scope of that url, as returned by scopeOf.
   */
  public forStored(label: string, source: string, scope: string | undefined): RDF.BlankNode {
    return this.lookup(label, source, scope);
  }

  protected lookup(label: string, source: string, scope: string | undefined, parsed?: RDF.BlankNode): RDF.BlankNode {
    let known = this.terms.get(label);
    if (known === undefined) {
      known = [];
      this.terms.set(label, known);
    }
    for (const entry of known) {
      if (scope === undefined ? entry.source === source : BlankNodeScopes.contains(scope, entry.source)) {
        return entry.term;
      }
    }
    // A parsed term is unique already, a stored label needs a prefix to be told apart from the same
    // label in another scope
    const term = parsed ?? this.dataFactory.blankNode(`sc${this.counter++}_${label}`);
    known.push({ source, term });
    return term;
  }

  /**
   * Whether `inner` lies under `outer`, where the boundary character keeps `/pods/15` from being
   * taken to contain `/pods/150`.
   */
  protected static contains(outer: string, inner: string): boolean {
    if (!inner.startsWith(outer)) {
      return false;
    }
    if (inner.length === outer.length || outer.endsWith('/') || outer.endsWith('#')) {
      return true;
    }
    const boundary = inner.charAt(outer.length);
    return boundary === '/' || boundary === '#' || boundary === '?';
  }
}
