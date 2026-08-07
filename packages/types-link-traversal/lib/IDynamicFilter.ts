/**
 * Defines a dynamic filter containing exact matches and pre-compiled regular expressions.
 */
export interface IDynamicFilter {
  addExact: (exactMatch: string) => void;
  addGlob: (selector: string) => void;
  matchesFilter: (url: string) => boolean;
}
