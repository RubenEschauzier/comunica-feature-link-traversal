import type { Algebra } from '@comunica/utils-algebra';
import type * as RDF from '@rdfjs/types';

function termKey(term: RDF.Term): string {
  return `${term.termType}:${term.value}`;
}

function addToIndex(index: Map<string, Algebra.Pattern[]>, key: string, pattern: Algebra.Pattern): void {
  const patterns = index.get(key);
  if (patterns) {
    patterns.push(pattern);
  } else {
    index.set(key, [ pattern ]);
  }
}

/**
 * Splits the patterns of a BGP into the maximal path-shaped sub-queries it contains.
 *
 * Two patterns are chained when the object of the one is the subject of the other and that term
 * links them unambiguously: exactly one pattern uses it as subject and exactly one uses it as
 * object. Where a term links more patterns the shape branches into a star, and the chain ends
 * there rather than forking. Every pattern therefore lands in at most one chain, so the composite
 * sources built from the chains cover disjoint sets of join entries. Sharing a term with patterns
 * outside the chain is fine, as the adaptive join controller joins the composite source with the
 * remaining entries on exactly those terms.
 *
 * Unlike the path of a derived resource itself, the terms of a chain need not be variables. A
 * constant subject or object only makes the chain more selective, and a constant that links two
 * patterns creates no join at all, so it can never introduce a branch two chains compete over.
 *
 * Patterns whose predicate is not a bound IRI are left out, as the parameterized query of a
 * derived resource is instantiated per predicate. Patterns that only occur on a cycle
 * (`?a <p> ?b . ?b <q> ?a`) are left out as well: they have no start to walk from, and a derived
 * resource chaining distinct variables cannot enforce the term that the cycle repeats.
 *
 * @param patterns The patterns of a BGP, in any order.
 * @returns The chains contained in the BGP, each in path order, pairwise pattern-disjoint.
 */
export function extractLinearSubqueries(patterns: Algebra.Pattern[]): Algebra.Pattern[][] {
  const candidates = patterns.filter(pattern => pattern.predicate.termType === 'NamedNode');

  const bySubject = new Map<string, Algebra.Pattern[]>();
  const byObject = new Map<string, Algebra.Pattern[]>();
  for (const pattern of candidates) {
    addToIndex(bySubject, termKey(pattern.subject), pattern);
    addToIndex(byObject, termKey(pattern.object), pattern);
  }

  /**
   * The pattern continuing the path at `term`, if `term` links exactly two patterns.
   * Returns undefined when the path ends at `term`, and when it branches there.
   */
  const continuationAt = (term: RDF.Term): Algebra.Pattern | undefined => {
    const key = termKey(term);
    const subjects = bySubject.get(key);
    if (subjects?.length !== 1 || byObject.get(key)?.length !== 1) {
      return undefined;
    }
    return subjects[0];
  };

  const chains: Algebra.Pattern[][] = [];
  const visited = new Set<Algebra.Pattern>();

  for (const candidate of candidates) {
    // Chains are only started at patterns that do not themselves continue another pattern, so
    // that every chain is walked from its start and comes out maximal
    if (continuationAt(candidate.subject) !== undefined) {
      continue;
    }

    const chain: Algebra.Pattern[] = [];
    let current: Algebra.Pattern | undefined = candidate;
    while (current && !visited.has(current)) {
      visited.add(current);
      chain.push(current);
      current = continuationAt(current.object);
    }
    chains.push(chain);
  }

  return chains;
}
