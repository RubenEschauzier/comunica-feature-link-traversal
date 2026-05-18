import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';
import { ActionContext } from '@comunica/core';
import type { ISourceState, ICacheMetrics, IPersistentCache } from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator } from 'asynciterator';
import { LRUCache } from 'lru-cache';
import { DataFactory } from 'rdf-data-factory';
import { RdfStore } from 'rdf-stores';
import { Factory } from 'sparqlalgebrajs';
import * as fs from 'fs';
import * as n3 from 'n3';
import { pipeline } from 'stream/promises';
import type * as RDF from '@rdfjs/types';
import { AlgebraFactory } from '@comunica/utils-algebra';


export class PersistentCacheSourceStateIndexed implements IPersistentCache<ISourceState> {
  private readonly sizeMap = new Map<string, number>();
  private readonly maxNumTriples: number;
  private readonly lruCacheDocuments: LRUCache<string, ISourceState>;

  private readonly dataFactory = new DataFactory();
  private readonly bindingsFactory = new BindingsFactory(this.dataFactory);
  private readonly algebraFactory = new AlgebraFactory(this.dataFactory);

  private isTracking = false;
  private cacheMetrics: ICacheMetrics;

  private readonly serializationLoc: string

  public constructor(args: IPersistentCacheSourceStateNumTriplesArgs) {
    this.maxNumTriples = args.maxNumTriples;
    this.lruCacheDocuments = new LRUCache<string, ISourceState>({
      maxSize: this.maxNumTriples,
      sizeCalculation: (value, key) => this.sizeMap.get(key) || 1,
      dispose: this.onDispose.bind(this),
    });
    this.serializationLoc = args.serializationLoc;
    this.cacheMetrics = this.resetMetrics();
  }

  public async get(key: string): Promise<ISourceState | undefined> {
    return this.getSync(key);
  }

  public getSync(key: string): ISourceState | undefined {
    const cachedState = this.lruCacheDocuments.get(key);

    if (this.isTracking) {
      cachedState ? this.cacheMetrics.hits++ : this.cacheMetrics.misses++;
    }

    return cachedState;
  }

  public async getMany(keys: string[]): Promise<(ISourceState | undefined)[]> {
    return keys.map(key => this.getSync(key));
  }

  /**
   * Upon setting of a source, we index it and set it in the LRUCache.
   * @param key
   * @param value
   * @returns
   */
  public async set(key: string, value: ISourceState): Promise<void> {
    const rdfStore = RdfStore.createDefault();
    const importStream = rdfStore.import(value.source.queryQuads(
      this.algebraFactory.createPattern(
        this.dataFactory.variable('s'),
        this.dataFactory.variable('p'),
        this.dataFactory.variable('o'),
        this.dataFactory.variable('g'),
      ),
      new ActionContext(),
    ));

    return new Promise((resolve, reject) => {
      importStream.on('end', () => {
        this.sizeMap.set(key, rdfStore.size);
        this.lruCacheDocuments.set(key, {
          ...value,
          source: new QuerySourceRdfJs(
            rdfStore,
            this.dataFactory,
            this.bindingsFactory,
          ),
        });
        resolve();
      });
      importStream.on('error', () => {
        reject('Import stream to cache error');
      });
    });
  }

  protected onDispose(value: ISourceState, key: string, reason: LRUCache.DisposeReason): void {
    if (reason === 'evict' && this.isTracking) {
      this.cacheMetrics.evictions++;
      this.cacheMetrics.evictionsCalculatedSize += this.sizeMap.get(key) ?? 1;
      this.cacheMetrics.evictionPercentage =
        (this.cacheMetrics.evictionsCalculatedSize / this.maxNumTriples) * 100;
      if (this.sizeMap.has(key)) {
        this.sizeMap.delete(key);
      }
    }
  }

  public async has(key: string): Promise<boolean> {
    return this.lruCacheDocuments.has(key);
  }

  public async delete(key: string): Promise<boolean> {
    return this.lruCacheDocuments.delete(key);
  }

  public entries(): AsyncIterator<[string, ISourceState]> {
    return new ArrayIterator(
      this.lruCacheDocuments.entries(),
      { autoStart: false },
    );
  }

  public async size(): Promise<number> {
    return this.lruCacheDocuments.calculatedSize;
  }

  public async serialize(): Promise<void> {
    if (!this.serializationLoc) return;

    // Two files: metadata (JSON) + quads (N-Quads, fast line-based format)
    const metaPath = `${this.serializationLoc}.meta.json`;
    const quadsPath = `${this.serializationLoc}.nq`;

    const metaEntries: any[] = [];

    try {
      const quadsStream = fs.createWriteStream(quadsPath);
      const writer = new n3.StreamWriter({ format: 'N-Quads' });

      quadsStream.on('error', (err) => { throw err; });

      writer.pipe(quadsStream);

      let sourceIndex = 0;

      let totalEntries = 0;
      let totalNonZeroDocuments = 0;
      let totalQuads = 0;
      let startTime = performance.now();
      for (const [key, sourceState] of this.lruCacheDocuments.entries()) {
        // Access quads directly if QuerySourceCacheWrapper exposes the store
        // Otherwise fall back to queryQuads — but avoid .toArray() by streaming directly
        const quadStream = sourceState.source.queryQuads(
          this.algebraFactory.createPattern(
            this.dataFactory.variable('s'),
            this.dataFactory.variable('p'),
            this.dataFactory.variable('o'),
            this.dataFactory.variable('g'),
          ),
          new ActionContext(),
        );

        // Tag each quad with a graph named after the cache key so we can 
        // reconstruct per-source boundaries without splitting files
        let quadCountDocument = 0;

        let graphAnnotationStream = quadStream.map((q: RDF.Quad) => {
          quadCountDocument++;
          const keyNode = this.dataFactory.namedNode(
            `urn:cache:${sourceIndex}:${encodeURIComponent(q.graph.value)}:${q.graph.termType}`
          );
          return this.dataFactory.quad(q.subject, q.predicate, q.object, keyNode)
        });

        await pipeline(
          graphAnnotationStream,
          writer,
          { end: false }
        )
        totalQuads += quadCountDocument;
        totalEntries++;
        if (quadCountDocument > 0){
          totalNonZeroDocuments++;
        }

        const plainHeaders: Record<string, string> = {};
        if (sourceState.headers instanceof Headers) {
          sourceState.headers.forEach((v, k) => { plainHeaders[k] = v; });
        }
        metaEntries.push({
          key,
          sourceIndex,
          quadCountDocument,
          url: sourceState.link.url,
          metadata: sourceState.metadata,
          handledDatasets: sourceState.handledDatasets,
          headers: plainHeaders,
        });
        sourceIndex += 1;
      }

      await new Promise<void>((resolve, reject) => {
        writer.end((error: any) => error ? reject(error) : resolve());
      });

      console.log(`Wrote ${totalQuads} quads in ${totalEntries} (${totalNonZeroDocuments} non-zero) 
        documents in ${(performance.now() - startTime)/1000} seconds.`)

      await fs.promises.writeFile(metaPath, JSON.stringify(metaEntries), 'utf8');
    } catch (error) {
      console.log('Serialization failed:', error);
      // Clean up partial files
      await Promise.allSettled([
        fs.promises.unlink(metaPath).catch(() => {
          console.log("Failed to clean partial metadata file")
        }),
        fs.promises.unlink(quadsPath).catch(() => {
          console.log("Failed to clean partial quads file")
        }),
      ]);
      throw error;
    }
  }

  public async deserialize(): Promise<void> {
    const metaPath = `${this.serializationLoc}.meta.json`;
    const quadsPath = `${this.serializationLoc}.nq`;

    if (!fs.existsSync(metaPath) || !fs.existsSync(quadsPath)) return;

    try {
      const startTime = performance.now();
      // Put quads in arrays based on the document they were dereferenced from
      const quadsByKey = new Map<string, RDF.Quad[]>();
      let readQuads = 0;

      await new Promise<void>((resolve, reject) => {
        const parser = new n3.StreamParser({ format: 'N-Quads' });
        const fileStream = fs.createReadStream(quadsPath);
        fileStream.pipe(parser);

        parser.on('data', (quad: RDF.Quad) => {
          readQuads++;
          // Graph name encodes the cache key
          const sourceIdxString = quad.graph.value.replace('urn:cache:', '');
          const splitIdxString = sourceIdxString.split(":");
          const sourceIndex = splitIdxString[0];

          if (!quadsByKey.has(sourceIndex)) {
            quadsByKey.set(sourceIndex, []);
          }

          const originalGraphValue = decodeURIComponent(splitIdxString[1]);
          const originalGraphTermType = splitIdxString[2];
          const originalQuad = this.dataFactory.quad(
            quad.subject,
            quad.predicate,
            quad.object,
            this.rehydrateGraphTerm(originalGraphValue, originalGraphTermType)
          );
          quadsByKey.get(sourceIndex)!.push(originalQuad);
        });
        parser.on('end', resolve);
        parser.on('error', reject);
      });

      // Reconstruct source states from saved metadata
      const metaEntries = JSON.parse(await fs.promises.readFile(metaPath, 'utf8'));
      let quadsLoaded = 0;
      for (const meta of metaEntries) {
        const sourceIdx = meta.sourceIndex.toString();
        let quadsArray = quadsByKey.get(sourceIdx);
      
        if (!quadsArray) {
          // If the metadata says the document should have quads somethng went wrong with serialization
          // and an error should be thrown
          if (meta.quadCountDocument > 0){
            throw new Error(`Found a metadata entry for URL ${meta.url} in rehydration with missing quads`);
          }
          // If we have a document without quads we still want to add it to cache as this prevents
          // having to re-request the document
          quadsArray = [];
        }
        const newIndexedSource = RdfStore.createDefault();
        for (const quad of quadsArray){
          const newQuad = newIndexedSource.addQuad(quad);
          if (newQuad){
            quadsLoaded++;
          }
        }
        this.sizeMap.set(meta.key, Math.max(newIndexedSource.size, 1));
        this.lruCacheDocuments.set(meta.key, {
          link: { url: meta.url },
          metadata: meta.metadata,
          handledDatasets: meta.handledDatasets,
          headers: meta.headers ? new Headers(meta.headers) : undefined,
          cachePolicy: {
            satisfiesWithoutRevalidation: async () => true,
          } as any,
          source: new QuerySourceRdfJs(
            newIndexedSource,
            this.dataFactory,
            this.bindingsFactory,
          ),
        });
      }

      // After rehydrating cache remove files to ensure proper shutdowns
      // don't retain cache content
      await Promise.all([
        fs.promises.unlink(metaPath),
        fs.promises.unlink(quadsPath),
      ]);

      console.log(`Rehydrated ${quadsLoaded} quads in ${metaEntries.length} documents in 
        ${(performance.now() - startTime)/1000} seconds.`);
      if (readQuads !== quadsLoaded){
        console.warn(`Cache rehydration count (${quadsLoaded}) not equal to read count (${readQuads})`);
      }
    } catch (error) {
      console.error('Deserialization failed:', error);
    }
  }

  private rehydrateGraphTerm(value: string, termType: string){
    switch (termType) {
      case 'NamedNode': 
        return this.dataFactory.namedNode(value);
      case 'BlankNode': 
        return this.dataFactory.blankNode(value);
      case 'Variable': 
        return this.dataFactory.variable(value);
      case 'DefaultGraph': 
        return this.dataFactory.defaultGraph();
    }
    throw new Error(`Invalid termType: ${termType}`)

  }

  public startSession() {
    this.isTracking = true;
    this.cacheMetrics = this.resetMetrics();
    return this.cacheMetrics;
  }

  public endSession() {
    this.isTracking = false;
    return this.cacheMetrics;
  }

  public resetMetrics(): ICacheMetrics {
    return {
      hits: 0,
      misses: 0,
      evictions: 0,
      evictionsCalculatedSize: 0,
      evictionPercentage: 0,
    };
  }
}

export interface IPersistentCacheSourceStateNumTriplesArgs {
  maxNumTriples: number;
  serializationLoc: string;
}
