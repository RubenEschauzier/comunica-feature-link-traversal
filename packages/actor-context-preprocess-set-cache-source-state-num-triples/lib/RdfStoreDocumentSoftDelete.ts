import type * as RDF from '@rdfjs/types';
import type { IRdfStoreOptions } from 'rdf-stores';
import { RdfStore } from 'rdf-stores';

/**
 * This is a stub implementation of a likely more efficient way of doing source-based caching of entire documents.
 * If this class gets actually fully implemented dependens on the efficiency of doing match calls over a cache
 * which just stores multiple smaller rdf-stores in its cache and iterates over them to match. It would look like this:
 * Build a streaming store here, add addition index mapping document to statements
 * (quads / ids mapping to quad strings?)
 * Then match can happen with additional filtering step that checks the index of
 * document to statement / statement to document to filter any matches that don't
 * satisfy the get key.
 * This class would then return quads instead that match the key.
 * Delete would happen by attaching a on evict function to LRU cache
 * that calls the delete method on all quads that match the deleted key
 * in the document index.
 * This natively supports cardinality estimation based on documents in traversal
 * as you call getMany on all document keys that are reachable according to your current
 * cache.
 */
export class RdfStoreDocumentSoftDelete extends RdfStore {
  protected documentMetadata: Map<string, IDocumentMetadata> = new Map();

  protected documentEncoding: Record<string, number> = {};
  protected encodingNum = 0;

  public constructor(options: IRdfStoreOptions<any, RDF.Quad>) {
    super(options);
  }

  /**
   * Adds a quad to the store with document metadata indicated
   * whether this document is deleted from the store of not.
   * @param quad
   * @returns
   */
  public addQuadDocument(quad: RDF.Quad, document: string): boolean {
    if (!this.documentMetadata.has(document)) {
      this.documentMetadata.set(document, { deleted: true });
    }
    const metadata = this.documentMetadata.get(document)!;
    // TODO This should be unified and probably the base class adjusted, as this will need
    // to accept an optional metadata field. Then that optional field should also be
    // accepted by the index to be set at the 'end' of a map. Then the get of the index
    // should also keep in mind this field and skip it if it says deleted. Finally
    // the index get should also delete any entries in the maps if that
    // entry is deleted.
    // Also the index should support compaction of its indexes based on tombstones by iterating
    // over all its entries, deleting any tombstones it finds and then this store
    // will delete all tombstones that say deleted: tru
    return super.addQuad(quad);
  }

  /**
   * Sets a document to deleted
   * @param document
   */
  public deleteDocument(document: string) {
    if (this.documentMetadata.has(document)) {
      this.documentMetadata.get(document)!.deleted = true;
    }
  }

  /**
   * Cleans up the underlying indexes and tombstones to remove
   * 'dead' quads from indexes and removes all deletion tombstones
   */
  public compact() {}

  private encodeDocument(document: string): number {
    if (this.documentEncoding[document]) {
      return this.documentEncoding[document];
    }
    this.documentEncoding[document] = this.encodingNum;
    this.encodingNum++;
    return this.encodingNum - 1;
  }
}

export interface IDocumentMetadata {
  deleted: boolean;
}
