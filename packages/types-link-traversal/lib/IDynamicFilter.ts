/**
 * Defines a dynamic filter containing exact matches and pre-compiled regular expressions.
 */
export interface IDynamicFilter {
  addExact: (exactMatch: string) => void;
  addGlob: (selector: string) => void;
  matchesFilter: (url: string) => boolean;
  /**
   * Hold back every url under a prefix until the returned function is called, for when what the
   * filter will say about them is not yet known.
   */
  hold: (prefix: string) => () => void;
  /**
   * Whether a url is currently held back.
   */
  isHeld: (url: string) => boolean;
  /**
   * Register a listener that is invoked whenever a hold is released.
   */
  addReleaseListener: (listener: () => void) => void;
}
