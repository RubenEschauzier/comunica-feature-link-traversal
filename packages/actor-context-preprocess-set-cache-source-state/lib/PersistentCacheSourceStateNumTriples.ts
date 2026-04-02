import { IActionQuerySourceIdentifyHypermedia, MediatorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import { ActionContext } from '@comunica/core';
import type { ISourceState, ICacheMetrics, IPersistentCache,  } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator } from 'asynciterator';
import { LRUCache } from 'lru-cache';
import { DataFactory } from 'rdf-data-factory';
import * as fs from 'fs';
import { KeysInitQuery } from '@comunica/context-entries';
import { QuerySourceCacheWrapper } from './QuerySourceCacheWrapper';
import type * as RDF from '@rdfjs/types';

export class PersistentCacheSourceStateNumTriples implements IPersistentCache<ISourceState> {
  private readonly sizeMap = new Map<string, number>();
  private readonly maxNumTriples: number;
  private readonly lruCacheDocuments: LRUCache<string, ISourceState>;

  private readonly dataFactory = new DataFactory();
  private readonly bindingsFactory = new BindingsFactory(this.dataFactory);
  private readonly algebraFactory = new AlgebraFactory(this.dataFactory);

  private readonly serializationLoc: string
  private readonly mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;


  private cacheMetrics: ICacheMetrics;

  public constructor(args: IPersistentCacheSourceStateNumTriplesArgs) {
    this.maxNumTriples = args.maxNumTriples;
    this.lruCacheDocuments = new LRUCache<string, ISourceState>({
      maxSize: this.maxNumTriples,
      sizeCalculation: (value, key) => this.sizeMap.get(key) || 1,
      dispose: this.onDispose.bind(this),
    });
    this.cacheMetrics = this.resetMetrics();
    this.serializationLoc = args.serializationLoc;
    this.mediatorQuerySourceIdentifyHypermedia = args.mediatorQuerySourceIdentifyHypermedia
  }

  public async get(key: string): Promise<ISourceState | undefined> {
    return this.getSync(key);
  }

  public getSync(key: string): ISourceState | undefined {
    const cachedState = this.lruCacheDocuments.get(key);

    // Track metrics
    cachedState ? this.cacheMetrics.hits++ : this.cacheMetrics.misses++;

    return cachedState;
  }

  public async getMany(keys: string[]): Promise<(ISourceState | undefined)[]> {
    return keys.map(key => this.getSync(key));
  }

  public async set(key: string, value: ISourceState): Promise<void> {
    this.lruCacheDocuments.set(key, value);
    if ('getSize' in value.source &&
            typeof value.source.getSize === 'function') {
      (<Promise<number>>value.source.getSize()).then((finalSize) => {
        if (this.lruCacheDocuments.has(key)) {
          this.sizeMap.set(key, finalSize);
          // We have to explicitly delete as .set() reuses the previous computed size
          this.lruCacheDocuments.delete(key);
          // Re-setting the key updates its size in the LRU engine
          this.lruCacheDocuments.set(key, value);
        }
      }).catch(() => {
        // Ignore stream errors here; they are handled by the main query consumer.
      });
    }
  }

  protected onDispose(value: ISourceState, key: string, reason: LRUCache.DisposeReason): void {
    if (reason === 'evict') {
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
      [ ...this.lruCacheDocuments.entries() ],
      { autoStart: false },
    );
  }

  public async size(): Promise<number> {
    return this.lruCacheDocuments.calculatedSize;
  }

  public async serialize(): Promise<void> {
    console.log("Serializing cache");
    if (!this.serializationLoc) {
      return;
    }

    const writeStream = fs.createWriteStream(this.serializationLoc, 'utf8');
    let success = false;

    // Fast-path write helper: avoids Promise overhead if the buffer is not full
    const writeAsync = (data: string): Promise<void> | void => {
      if (writeStream.write(data)) return;
      
      return new Promise((resolve, reject) => {
        const onDrain = () => {
          writeStream.removeListener('error', onError);
          resolve();
        };
        const onError = (error: Error) => {
          writeStream.removeListener('drain', onDrain);
          reject(error);
        };
        writeStream.once('drain', onDrain);
        writeStream.once('error', onError);
      });
    };

    try {
      console.log(`Serializing total triples: ${this.lruCacheDocuments.calculatedSize}`);
      const initWrite = writeAsync('[\n');
      if (initWrite) await initWrite;

      const entries = Array.from(this.lruCacheDocuments.entries());
      const batchSize = 25; // Adjust this size based on available memory and concurrency limits
      let isFirst = true;

      for (let i = 0; i < entries.length; i += batchSize) {
        const batch = entries.slice(i, i + batchSize);

        // Process quads and transform objects concurrently within the batch
        const serializedBatch = await Promise.all(batch.map(async ([key, sourceState]) => {
          
          const quadsArray = await sourceState.source.queryQuads(
            this.algebraFactory.createPattern(
              this.dataFactory.variable('s'),
              this.dataFactory.variable('p'),
              this.dataFactory.variable('o'),
              this.dataFactory.variable('g'),
            ),
            new ActionContext(),
          ).toArray();

          const plainHeaders: Record<string, string> = {};
          if (sourceState.headers instanceof Headers) {
            sourceState.headers.forEach((value, name) => {
              plainHeaders[name] = value;
            });
          }

          const querySourceActionData: IActionQuerySourceIdentifyHypermedia = {
            url: sourceState.link.url,
            metadata: sourceState.metadata,
            quads: <any> quadsArray,
            handledDatasets: sourceState.handledDatasets,
            context: <any> {}, 
          };

          const sourceStateData: any = {
            ...sourceState,
            source: querySourceActionData,
            metadata: sourceState.metadata,
            headers: plainHeaders,
          };

          return JSON.stringify([key, sourceStateData]);
        }));

        // Write the processed batch sequentially to the stream
        for (const jsonString of serializedBatch) {
          const data = `${isFirst ? '' : ',\n'}${jsonString}`;
          isFirst = false;
          
          const writePromise = writeAsync(data);
          if (writePromise) await writePromise;
        }
      }

      const endWrite = writeAsync('\n]');
      if (endWrite) await endWrite;
      
      success = true;
    } catch (error) {
      console.error(`Failed to stream cache serialization to ${this.serializationLoc}:`, error);
    } finally {
      await new Promise<void>((resolve) => writeStream.end(resolve));

      // Delete the file if serialization failed mid-stream to prevent corrupted caches
      if (!success && fs.existsSync(this.serializationLoc)) {
        await fs.promises.unlink(this.serializationLoc);
        console.log("Deleted corrupted cache file.");
      }
    }
  }
  
  private rehydrateTerm(term: any): RDF.Term {
    if (!term) return term;
    switch (term.termType) {
      case 'NamedNode': 
        return this.dataFactory.namedNode(term.value);
      case 'BlankNode': 
        return this.dataFactory.blankNode(term.value);
      case 'Literal': 
        return this.dataFactory.literal(
          term.value, 
          term.language || (term.datatype ? this.rehydrateTerm(term.datatype) : undefined)
        );
      case 'Variable': 
        return this.dataFactory.variable(term.value);
      case 'DefaultGraph': 
        return this.dataFactory.defaultGraph();
      case 'Quad': 
        return this.dataFactory.quad(
          <RDF.Quad_Subject>this.rehydrateTerm(term.subject),
          <RDF.Quad_Predicate>this.rehydrateTerm(term.predicate),
          <RDF.Quad_Object>this.rehydrateTerm(term.object),
          <RDF.Quad_Graph>this.rehydrateTerm(term.graph)
        );
      default: 
        return term;
    }
  }

  public async deserialize(): Promise<void> {
    if (!this.serializationLoc || !fs.existsSync(this.serializationLoc)) {
      return;
    }

    try {
      const fileData = await fs.promises.readFile(this.serializationLoc, 'utf8');
      const parsedEntries: [string, IQuerySourceSerialization][] = JSON.parse(fileData);

      for (const [key, sourceStateData] of parsedEntries) {
        // Reconstruct Headers object
        const reconstructedHeaders = sourceStateData.headers 
          ? new Headers(sourceStateData.headers as unknown as Record<string, string>) 
          : undefined;

        if (sourceStateData.source && Array.isArray(sourceStateData.source.quads)) {
          sourceStateData.source.quads = new ArrayIterator(sourceStateData.source.quads.map(quad => 
            <RDF.Quad>this.rehydrateTerm(quad)
          ));
        }

        const output = await this.mediatorQuerySourceIdentifyHypermedia.mediate(
          { 
            ...sourceStateData.source,
            context: ActionContext.ensureActionContext().set(KeysInitQuery.dataFactory, this.dataFactory),
          }
        );
        const source = new QuerySourceCacheWrapper(output.source);

        // Reconstruct the cache entry with the mocked cachePolicy
        const fullState: ISourceState = {
          ...sourceStateData,
          source,
          headers: reconstructedHeaders,
          cachePolicy: <any> {
            satisfiesWithoutRevalidation: async (action: any): Promise<boolean> => true,
          },
        };

        this.lruCacheDocuments.set(key, fullState);
      }
      // Remove after deserializing, as this is mainly for rehydration after timeouts, which to prevent
      // stale data being read should be a destructive operation.
      await fs.promises.unlink(this.serializationLoc);
      
      console.log(`Deserialized cache has ${parsedEntries.length} documents` );
    } catch (error) {
      console.error(`Failed to deserialize cache from ${this.serializationLoc}:`, error);
    }

  }
  public startSession() {
    console.log(`Start new tracking session.`);
    this.cacheMetrics = this.resetMetrics();
    return this.cacheMetrics;
  }

  public endSession() {
    console.log(`End tracking session`);
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
  mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia
}

export interface IQuerySourceSerialization extends Omit<ISourceState, 'source'> {
  source: IActionQuerySourceIdentifyHypermedia;
  // cachePolicy: ISerializedCachePolicyHttp
}
