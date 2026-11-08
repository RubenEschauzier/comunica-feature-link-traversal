import type { FragmentSelectorShape } from '@comunica/types';
import type { Algebra } from '@comunica/utils-algebra';
import { canAnswerBgp, doesShapeAcceptOperation } from '@comunica/utils-query-operation';

/**
 * Whether a resource with the given selector shape answers the bgp in one request.
 *
 * Resources come with two kinds of selector shape, which are matched differently:
 * - A template: a parameterized resource answers exactly one query, the bgp of its shape, with
 *   some of the terms filled in. The bgp is answered when it is an instance of that template,
 *   pattern for pattern, which `canAnswerBgp` decides. A QPF interface also has a pattern for its
 *   shape, but a single pattern rather than a bgp, so it answers no bgp of more than one pattern.
 * - A capability: an interface such as a SPARQL endpoint has a shape saying what kinds of
 *   operation it accepts, such as a wildcard accepting all of them. This says nothing about how
 *   many patterns a bgp has, so the bgp is answered when the shape accepts it as an operation.
 *
 * A template is matched as a capability would be when it is not a pattern, as then there is no
 * template to fill in.
 * @param shape The selector shape of the resource.
 * @param bgp The bgp to answer.
 */
export function canShapeAnswerBgp(shape: FragmentSelectorShape, bgp: Algebra.Bgp): boolean {
  if (shape.type === 'operation' && shape.operation.operationType === 'pattern') {
    return canAnswerBgp(shape, bgp, shape.variablesOptional ?? [], shape.variablesRequired ?? []);
  }
  return doesShapeAcceptOperation(shape, bgp);
}
