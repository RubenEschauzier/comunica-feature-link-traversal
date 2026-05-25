import * as fs from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';
import { ActionContext } from '@comunica/core';
import type { ISourceState, ICacheMetrics, IPersistentCache } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator } from 'asynciterator';
import { LRUCache } from 'lru-cache';
import * as n3 from 'n3';
import { DataFactory } from 'rdf-data-factory';
import { RdfStore } from 'rdf-stores';

export class PersistentCacheCset implements IPersistentCache<any, any> {

  private readonly dataFactory = new DataFactory();
  private readonly bindingsFactory = new BindingsFactory(this.dataFactory);
  private readonly algebraFactory = new AlgebraFactory(this.dataFactory);

  private isTracking = false;
  private cacheMetrics: ICacheMetrics;

  private readonly serializationLoc: string;

  public constructor(args: IPersistentCacheSourceStateNumTriplesArgs) {
    this.serializationLoc = args.serializationLoc;
    this.cacheMetrics = this.resetMetrics();
  }

  public async get(key: string): Promise<ISourceState | undefined> {
  }

  public getSync(key: string): ISourceState | undefined {
  }

  public async getMany(keys: string[]): Promise<(ISourceState | undefined)[]> {
  }

  /**
   * Upon setting of a source, we index it and set it in the LRUCache.
   * @param key
   * @param value
   * @returns
   */
  public async set(key: string, value: ISourceState): Promise<void> {
  }

  protected onDispose(value: ISourceState, key: string, reason: LRUCache.DisposeReason): void {
  }

  public async has(key: string): Promise<boolean> {
  }

  public async delete(key: string): Promise<boolean> {
  }

  public entries(): AsyncIterator<[string, ISourceState]> {
  }

  public async size(): Promise<number> {
  }

  public async serialize(): Promise<void> {
  }

  public async deserialize(): Promise<void> {
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
