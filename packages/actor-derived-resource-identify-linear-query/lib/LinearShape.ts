import type { Algebra } from '@comunica/utils-algebra';

/**
 * Brings the patterns of a BGP into the order of the path they describe, if they describe one.
 *
 * The patterns form a path if all subjects and objects are variables, every variable is used
 * at most once as subject and at most once as object, and following the objects from the one
 * subject that is never an object visits all patterns. For example, these patterns:
 *
 * WHERE {
 *  ?o1 ?p2 ?o2
 *  ?s  ?p1 ?o1
 * }
 *
 * are ordered as `?s ?p1 ?o1`, `?o1 ?p2 ?o2`.
 *
 * @param patterns The patterns of a BGP, in any order.
 * @returns The patterns ordered from the start of the path, or undefined if they are not a path.
 */
export function findLinearOrder(patterns: Algebra.Pattern[]): Algebra.Pattern[] | undefined {
  if (patterns.length < 2) {
    return;
  }

  const patternBySubject = new Map<string, Algebra.Pattern>();
  const objects = new Set<string>();

  for (const pattern of patterns) {
    // A path only chains variables, constants would make the links between patterns fixed
    if (pattern.subject.termType !== 'Variable' || pattern.object.termType !== 'Variable') {
      return;
    }
    // Reusing a variable as subject or as object makes the shape branch instead of chain
    if (patternBySubject.has(pattern.subject.value) || objects.has(pattern.object.value)) {
      return;
    }
    patternBySubject.set(pattern.subject.value, pattern);
    objects.add(pattern.object.value);
  }

  // The start of the path is the only subject that is not an object of another pattern
  const roots = [ ...patternBySubject.keys() ].filter(subject => !objects.has(subject));
  if (roots.length !== 1) {
    return;
  }

  const ordered: Algebra.Pattern[] = [];
  let current: Algebra.Pattern | undefined = patternBySubject.get(roots[0]);
  while (current) {
    ordered.push(current);
    current = patternBySubject.get(current.object.value);
  }

  // Patterns that are not on the path from the root form a separate cycle
  if (ordered.length !== patterns.length) {
    return;
  }

  return ordered;
}
