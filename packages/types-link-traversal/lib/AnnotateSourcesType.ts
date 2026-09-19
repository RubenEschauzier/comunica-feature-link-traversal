/**
 * How the documents that asserted a triple are recorded.
 *
 * - `graph`: the graph of every quad is rewritten to the document it came from. Provenance then
 *   sits inside the identity of the quad, so one triple asserted by two documents is two quads.
 * - `index`: the quad is stored as it was read and the document is recorded beside it in the
 *   aggregated store. One triple stays one quad however many documents deliver it.
 */
export type AnnotateSourcesType = 'graph' | 'index';
