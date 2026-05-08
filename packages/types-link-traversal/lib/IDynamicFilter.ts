/**
 * Defines a dynamic filter containing exact matches and pre-compiled regular expressions.
 */
export interface IDynamicFilter {
  readonly exact: Set<string>;
  readonly regExp: RegExp[];
}