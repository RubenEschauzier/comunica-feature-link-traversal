import { ILink } from "@comunica/types";

/**
 * Interface containing data for offline traversal
 */
export interface IOfflineTraversalEntry {
  /**
   * The traversal entries depending on predicates in the query
   */
  predicates: Record<string, ILink>;
  /**
   * The traversal entries independent of the query
   */
  default: ILink[];
}