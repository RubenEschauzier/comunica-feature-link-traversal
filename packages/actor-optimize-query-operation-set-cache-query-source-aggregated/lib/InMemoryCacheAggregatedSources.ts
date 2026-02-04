import { QuerySourceCacheWrapper } from '../../actor-context-preprocess-set-cache-source-state/lib';
import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';
import { ActionContext } from '@comunica/core';
import type { ISourceState } from '@comunica/types';
import type { IPersistentCache } from '@comunica/types-link-traversal';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator } from 'asynciterator';
import { LRUCache } from 'lru-cache';
import { DataFactory } from 'rdf-data-factory';
import { RdfStore } from 'rdf-stores';
import { Factory } from 'sparqlalgebrajs';

export class PersistentCacheSourceStateIndexed implements IPersistentCache<ISourceState> {
  private readonly sizeMap = new Map<string, number>();
  private readonly lruCacheDocuments: LRUCache<string, string>;

  public readonly DF: DataFactory = new DataFactory();
  public readonly AF: Factory = new Factory(this.DF);
  public readonly BF: BindingsFactory = new BindingsFactory(this.DF, {});

  public constructor(args: IPersistentCacheSourceStateNumTriplesArgs) {
    this.lruCacheDocuments = new LRUCache<string, ISourceState>({
      maxSize: args.maxNumTriples,
      sizeCalculation: (value, key) => this.sizeMap.get(key) || 1,
    });
  }

  public async get(key: string): Promise<ISourceState | undefined> {
    return this.lruCacheDocuments.get(key);
  }

  public async getMany(keys: string[]): Promise<(ISourceState | undefined)[]> {
    return keys.map(key => this.lruCacheDocuments.get(key));
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
          this.AF.createPattern(
            this.DF.variable('s'),
            this.DF.variable('p'),
            this.DF.variable('o'),
            this.DF.variable('g'),
          ),
          new ActionContext(),
        ));
    
    return new Promise((resolve, reject) => {
      importStream.on('end', () => {
        this.sizeMap.set(key, rdfStore.size);
        this.lruCacheDocuments.set(key, 
          { 
            ...value,
            source: new QuerySourceRdfJs(
              rdfStore,
              this.DF,
              this.BF
            )
          }
        )
        resolve()
      });
      importStream.on('error', () => {
        reject('Import stream error')
      });
    })
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

  public serialize(): Promise<void> {
    throw new Error('Serialize implemented for this in-memory cache');
  }
}

export interface IPersistentCacheSourceStateNumTriplesArgs {
  maxNumTriples: number;
}
