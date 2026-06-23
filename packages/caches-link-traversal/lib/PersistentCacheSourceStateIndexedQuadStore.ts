import * as fs from 'node:fs/promises';
import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';
import { ActionContext } from '@comunica/core';
import type { ISourceState, ICacheMetrics, IPersistentCache } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import type * as RDF from '@rdfjs/types';
import type { Quad } from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator } from 'asynciterator';
import { LRUCache } from 'lru-cache';
import { DataFactory } from 'rdf-data-factory';
import { RdfStore } from 'rdf-stores';
import { pipeline } from 'node:stream/promises';
import * as fsSync from 'node:fs';
import * as path from 'node:path';
import * as readline from 'node:readline';

export class PersistentCacheSourceStateIndexedQuadStore implements IPersistentCache<ISourceState, ISourceState> {
  private readonly maxNumTriplesStore: number;
  private readonly activeIngestions = new Map<string, Promise<void>>();

  private readonly lruCacheStoreBacked: LRUCache<string, string>;
  private readonly metadataKeysToCache: string[];
  private readonly savedMetadata = new Map<string, Record<string, any>>();
  private readonly sizeMap = new Map<string, number>();

  private readonly dataFactory = new DataFactory();
  private readonly bindingsFactory = new BindingsFactory(this.dataFactory);
  private readonly algebraFactory = new AlgebraFactory(this.dataFactory);

  private isTracking = false;
  private cacheMetrics: ICacheMetrics;

  private readonly serializationLoc: string;
  private isClosed: boolean;
  private store: RdfStore;

  public constructor(args: ICacheInMemoryArgs) {
    this.maxNumTriplesStore = args.maxNumTriples;
    this.serializationLoc = args.serializationLoc ?? `${__dirname}/../cache/`;

    // Instantiate a standalone store tied to this cache instance
    this.store = RdfStore.createDefault();
    this.isClosed = false;

    this.lruCacheStoreBacked = new LRUCache<string, string>({
      maxSize: this.maxNumTriplesStore,
      sizeCalculation: (value, key) => this.sizeMap.get(key) || 1,
      dispose: this.onDispose.bind(this),
    });

    this.metadataKeysToCache = args.metadataKeysToCache ?? 
        [ 'traverse', 'defaultTraversal', 'predicateToLinks' ];

    this.cacheMetrics = this.resetMetrics();
  }

  private getCacheGraphNode(url: string): RDF.NamedNode {
    return this.dataFactory.namedNode(`urn:cache:doc:${encodeURIComponent(url)}`);
  }

  public async getMany(keys: string[]): Promise<(ISourceState | undefined)[]> {
    return Promise.all(keys.map(key => this.get(key)));
  }

  public async get(key: string): Promise<ISourceState | undefined> {
    const ongoingIngestion = this.activeIngestions.get(key);
    if (ongoingIngestion) {
      await ongoingIngestion;
    }

    const cacheGraph = this.getCacheGraphNode(key);
    
    const quads = this.store.getQuads(undefined, undefined, undefined, cacheGraph);
    if (quads.length > 0) {
      if (this.isTracking) {
        this.cacheMetrics.hits++;
      }
      const rehydratedState = this.createSourceStateFromMemory(key, cacheGraph);

      const storedMeta = this.savedMetadata.get(key);
      if (storedMeta === undefined) {
        throw new Error(`Could not find saved metadata for cache entry ${key}`);
      }

      rehydratedState.metadata = { ...rehydratedState.metadata, ...storedMeta };
      return rehydratedState;
    }

    if (this.isTracking) {
      this.cacheMetrics.misses++;
    }
    return undefined;
  }

  public async set(key: string, value: ISourceState): Promise<void> {
    if (this.isClosed){
      return;
    }

    if (this.activeIngestions.has(key)) {
      return this.activeIngestions.get(key);
    }

    const ingestionPromise = this._set(key, value).finally(() => {
      this.activeIngestions.delete(key);
    });

    this.activeIngestions.set(key, ingestionPromise);
    await ingestionPromise;
  }

  private async _set(key: string, value: ISourceState): Promise<void> {
    const cacheGraph = this.getCacheGraphNode(key);

    const quadStream = value.source.queryQuads(
      this.algebraFactory.createPattern(
        this.dataFactory.variable('s'),
        this.dataFactory.variable('p'),
        this.dataFactory.variable('o'),
        this.dataFactory.variable('g'),
      ),
      new ActionContext(),
    );

    let nTriples = 0;
    const predicateToLinks: Record<string, Set<string>> = {};
    const extractLinks = this.metadataKeysToCache.includes('predicateToLinks');

    const transformStream = quadStream.map((quad) => {
      nTriples++;
      if (extractLinks && quad.object.termType === 'NamedNode') {
        let urlSet = predicateToLinks[quad.predicate.value];
        if (!urlSet) {
          urlSet = new Set<string>();
          predicateToLinks[quad.predicate.value] = urlSet;
        }
        urlSet.add(quad.object.value);
      }
      return this.dataFactory.quad(quad.subject, quad.predicate, quad.object, cacheGraph);
    });

    await new Promise<void>((resolve, reject) => {
      transformStream.on('data', (quad) => this.store.addQuad(quad));
      transformStream.on('end', resolve);
      transformStream.on('error', reject);
    });

    this.sizeMap.set(key, nTriples);

    if (extractLinks) {
      const serializableLinks: Record<string, string[]> = {};
      for (const [predicate, urlSet] of Object.entries(predicateToLinks)) {
        serializableLinks[predicate] = Array.from(urlSet);
      }
      value.metadata.predicateToLinks = serializableLinks;
    }

    const extractedMetadata: Record<string, any> = {};
    for (const metaKey of this.metadataKeysToCache) {
      if (value.metadata[metaKey] !== undefined) {
        extractedMetadata[metaKey] = value.metadata[metaKey];
      }
    }
    this.savedMetadata.set(key, extractedMetadata);

    this.lruCacheStoreBacked.set(key, '');
  }

  private createSourceStateFromMemory(key: string, cacheGraph: RDF.NamedNode): ISourceState {
    const sanitizeTerm = <T extends RDF.Term>(term?: T): T | undefined =>
      term?.termType === 'Variable' ? undefined : term;

    const quadSource: any = {
      match: (
        subject?: RDF.Quad_Subject | undefined,
        predicate?: RDF.Quad_Predicate | undefined,
        object?: RDF.Quad_Object | undefined,
        graph?: RDF.Quad_Graph | undefined,
      ): AsyncIterator<Quad> => {
        const quads = this.store.getQuads(
          sanitizeTerm(subject),
          sanitizeTerm(predicate),
          sanitizeTerm(object),
          cacheGraph,
        );
        const mappedQuads = quads.map(quad => {
            quad.graph = this.dataFactory.defaultGraph();
            return quad;
        });
        return new ArrayIterator(mappedQuads);
      },
      countQuads: async(
        subject?: RDF.Quad_Subject | undefined,
        predicate?: RDF.Quad_Predicate | undefined,
        object?: RDF.Quad_Object | undefined,
        graph?: RDF.Quad_Graph | undefined,
      ): Promise<number> =>
        this.store.countQuads(
          sanitizeTerm(subject),
          sanitizeTerm(predicate),
          sanitizeTerm(object),
          cacheGraph,
        ),
    };
    
    const cachedMetadata = this.savedMetadata.get(key);

    return {
      link: { url: key },
      metadata: {
        traverse: [],
        state: new MetadataValidationState(),
        cardinality: { value: 0, type: 'estimate' },
        variables: [],
        ...cachedMetadata
      },
      handledDatasets: {},
      cachePolicy: <any> { satisfiesWithoutRevalidation: async() => true },
      source: new QuerySourceRdfJs(
        quadSource,
        this.dataFactory,
        this.bindingsFactory,
      ),
    };
  }

  protected onDispose(value: string, key: string, reason: LRUCache.DisposeReason) {
    if (reason === 'evict' && this.isTracking) {
      this.cacheMetrics.evictions++;
      this.cacheMetrics.evictionsCalculatedSize += this.sizeMap.get(key) ?? 1;
      this.cacheMetrics.evictionPercentage =
        (this.cacheMetrics.evictionsCalculatedSize / this.maxNumTriplesStore) * 100;
      this._delete(key);
    }
  }

  public async has(key: string): Promise<boolean> {
    return this.store.getQuads(undefined, undefined, undefined, this.getCacheGraphNode(key)).length > 0;
  }

  public async delete(key: string): Promise<boolean> {
    const ongoingIngestion = this.activeIngestions.get(key);
    if (ongoingIngestion) {
      try {
        await ongoingIngestion;
      } catch {
        // Proceed with cascade to clean up memory limits
      }
    }
    this.sizeMap.delete(key);
    return this._delete(key);
  }

  private _delete(key: string): Promise<boolean> {
    this.lruCacheStoreBacked.delete(key);
    this.savedMetadata.delete(key);

    const result = this.store.deleteGraph(this.getCacheGraphNode(key));
    return new Promise<boolean>((resolve) => {
      result.on('end', () => resolve(true));
      result.on('error', () => resolve(false));
    });
  }

  public async serialize(): Promise<void> {
    this.isClosed = true;

    if (this.activeIngestions.size > 0) {
      await Promise.allSettled([...this.activeIngestions.values()]);
    }

    try {
      const metadataFile = path.join(this.serializationLoc, 'metadata.jsonl');
      const storeFile = path.join(this.serializationLoc, 'store.jsonl');

      // Serialize metadata
      const writeStreamMeta = fsSync.createWriteStream(metadataFile, { encoding: 'utf-8' });
      
      const generateMetadata = async function* (this: PersistentCacheSourceStateIndexedQuadStore) {
        yield JSON.stringify({ type: 'lruStore', data: this.lruCacheStoreBacked.dump() }) + '\n';
        
        for (const [key, value] of this.sizeMap.entries()) {
          yield JSON.stringify({ type: 'sizeMap', key, value }) + '\n';
        }

        for (const [key, value] of this.savedMetadata.entries()) {
          yield JSON.stringify({ type: 'savedMetadata', key, value }) + '\n';
        }
      };

      await pipeline(generateMetadata.bind(this)(), writeStreamMeta);

      // Serialize quads in store
      let nSerialized = 0;
      const storeWriteStream = fsSync.createWriteStream(storeFile, { encoding: 'utf-8' });
      const quadStream = this.store.match();
      
      const generateQuads = async function* () {
        for await (const quad of quadStream) {
          nSerialized++;
          yield JSON.stringify(quad) + '\n';
        }
      };

      await pipeline(generateQuads(), storeWriteStream);

      console.log(`Successfully serialized cache metadata and store with ${nSerialized} triples`);
    } catch (error) {
      console.error('Failed to serialize cache state:', error);
    }
  }
  
  // public async serialize(): Promise<void> {
  //   try {
  //     const metadataFile = path.join(this.serializationLoc, 'metadata.json');

  //     const serializedData = {
  //       lruCacheStoreBacked: this.lruCacheStoreBacked.dump(),
  //       sizeMap: [ ...this.sizeMap.entries() ],
  //       savedMetadata: [ ...this.savedMetadata.entries() ],
  //     };

  //     await fs.writeFile(metadataFile, JSON.stringify(serializedData), 'utf-8');
  //     console.log(`Successfully serialized cache metadata to ${metadataFile}`);
  //   } catch (error) {
  //     console.error('Failed to serialize cache metadata:', error);
  //   }
  // }
  public async deserialize(): Promise<void> {
    const metadataFile = path.join(this.serializationLoc, 'metadata.jsonl');
    const storeFile = path.join(this.serializationLoc, 'store.jsonl');

    try {
      await fs.access(metadataFile);
    } catch {
      return; 
    }

    this.sizeMap.clear();
    this.savedMetadata.clear();
    this.store = RdfStore.createDefault();

    try {
      // Deserialize Metadata
      const readStreamMeta = fsSync.createReadStream(metadataFile, { encoding: 'utf-8' });
      const rlMeta = readline.createInterface({ input: readStreamMeta, crlfDelay: Infinity });

      for await (const line of rlMeta) {
        if (!line.trim()) continue;
        const parsed = JSON.parse(line);

        switch (parsed.type) {
          case 'lruStore':
            this.lruCacheStoreBacked.load(parsed.data);
            break;
          case 'sizeMap':
            this.sizeMap.set(parsed.key, parsed.value);
            break;
          case 'savedMetadata':
            this.savedMetadata.set(parsed.key, parsed.value);
            break;
        }
      }

      // Deserialize Store Quads
      let nDeserialized = 0;
      try {
        await fs.access(storeFile);

        const readStreamStore = fsSync.createReadStream(storeFile, { encoding: 'utf-8' });
        const rlStore = readline.createInterface({ input: readStreamStore, crlfDelay: Infinity });

        for await (const line of rlStore) {
          if (!line.trim()) continue;
          nDeserialized++;
          const parsedQuad = JSON.parse(line);
          const quad = this.dataFactory.quad(
            <RDF.Quad_Subject>this.reconstructTerm(parsedQuad.subject),
            <RDF.Quad_Predicate>this.reconstructTerm(parsedQuad.predicate),
            <RDF.Quad_Object>this.reconstructTerm(parsedQuad.object),
            <RDF.Quad_Graph>this.reconstructTerm(parsedQuad.graph)
          );
          this.store.addQuad(quad);
        }
      } catch {
        console.warn('Store file not found or corrupted; starting with empty store.');
      }

      console.log(`Successfully deserialized cache metadata and store with ${nDeserialized} triples`);
    } catch (error) {
      console.error('Failed to deserialize cache state:', error);
    }
  }

  // public async deserialize(): Promise<void> {
  //   try {
  //     const metadataFile = path.join(this.serializationLoc, 'metadata.json');

  //     try {
  //       await fs.access(metadataFile);
  //     } catch {
  //       return;
  //     }

  //     const rawData = await fs.readFile(metadataFile, 'utf-8');
  //     const parsedData = JSON.parse(rawData);

  //     if (parsedData.sizeMap) {
  //       this.sizeMap.clear();
  //       for (const [ k, v ] of parsedData.sizeMap) {
  //         this.sizeMap.set(k, v);
  //       }
  //     }

  //     if (parsedData.savedMetadata) {
  //       this.savedMetadata.clear(); ;
  //       for (const [ k, v ] of parsedData.savedMetadata) {
  //         this.savedMetadata.set(k, v);
  //       }
  //     }

  //     if (parsedData.lruCacheStoreBacked) {
  //       this.lruCacheStoreBacked.load(parsedData.lruCacheStoreBacked);
  //     }

  //     console.log(`Successfully deserialized cache metadata from ${metadataFile}`);
  //   } catch (error) {
  //     console.error('Failed to deserialize cache metadata:', error);
  //   }
  // }

  protected reconstructTerm(termObj: any): RDF.Term {
    switch (termObj.termType) {
      case 'NamedNode':
        return this.dataFactory.namedNode(termObj.value);
      case 'BlankNode':
        return this.dataFactory.blankNode(termObj.value);
      case 'Literal': {
        const datatype = termObj.datatype ? this.dataFactory.namedNode(termObj.datatype.value) : undefined;
        return this.dataFactory.literal(termObj.value, termObj.language || datatype);
      }
      case 'DefaultGraph':
        return this.dataFactory.defaultGraph();
      case 'Variable':
        return this.dataFactory.variable(termObj.value);
      default:
        throw new Error(`Unsupported term type: ${termObj.termType}`);
    }
  }

  public async clear(): Promise<void> {
    const pendingIngestions = [ ...this.activeIngestions.values() ];
    if (pendingIngestions.length > 0) {
      await Promise.allSettled(pendingIngestions);
    }

    this.sizeMap.clear();
    this.savedMetadata.clear();
    this.lruCacheStoreBacked.clear();
    this.activeIngestions.clear();

    // Reinstantiate the local memory store to drop all data
    this.store = RdfStore.createDefault();
  }

  public entries(): AsyncIterator<[string, ISourceState]> {
    const entries = Array.from(this.sizeMap.keys()).map((key): [string, ISourceState] => {
      const cacheGraph = this.getCacheGraphNode(key);
      return [ key, this.createSourceStateFromMemory(key, cacheGraph) ];
    });
    return new ArrayIterator(entries, { autoStart: false });
  }

  public async size(): Promise<number> {
    return this.store.size;
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

export interface ICacheInMemoryArgs {
  maxNumTriples: number;
  serializationLoc?: string;
  metadataKeysToCache?: string[];
}