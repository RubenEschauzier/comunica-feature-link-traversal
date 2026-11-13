# Comunica Symmetric RDF Join Actors

Two rdf-join actors that stream both join inputs symmetrically, so results come out as soon as both of
their bindings have arrived and neither input has to end first:

* `ActorRdfJoinInnerSymmetricCartesian` (`inner`, `symmetric-cartesian`): two entries without shared variables.
* `ActorRdfJoinOptionalSymmetricHash` (`optional`, `symmetric-hash`): OPTIONAL, keyed on the shared variables
  (none for a cartesian product). A left binding that joins with nothing is emitted once the right input has ended.

Comunica's nested-loop, hash and bind joins emit per left binding in order and finish a left binding only
once the right input has ended. Under link traversal no input ends before traversal does, so those joins
hold back almost all results until then.
