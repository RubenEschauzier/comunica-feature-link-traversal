import { ActionContext } from '@comunica/core';
import type { ISourceState, ICacheMetrics, IPersistentCache } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import type { LRUCache } from 'lru-cache';
import { DataFactory } from 'rdf-data-factory';
import * as RdfString from 'rdf-string';

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

  public async get(key: string): Promise<ISourceState | undefined> {}

  public getSync(key: string): ISourceState | undefined {}

  public async getMany(keys: string[]): Promise<(ISourceState | undefined)[]> {}

  /**
   * Upon setting of a source, we create a cset for this document
   * @param key
   * @param value
   * @returns
   */
  public async set(key: string, value: ISourceState): Promise<void> {
    const quads = value.source.queryQuads(
      this.algebraFactory.createPattern(
        this.dataFactory.variable('s'),
        this.dataFactory.variable('p'),
        this.dataFactory.variable('o'),
        this.dataFactory.variable('g'),
      ),
      new ActionContext(),
    );

    // Quads are not guaranteed grouped by subject, so we need to do a
    // pass to group them
    const subjectToProperties = new Map<string, Map<string, number>>();
    try {
      for await (const quad of quads) {
        const { subject, predicate } = quad;

        const subjectKey = subject.termType === 'Quad' ?
          JSON.stringify(RdfString.quadToStringQuad(subject)) :
          RdfString.termToString(subject);

        let properties = subjectToProperties.get(subjectKey);
        if (!properties) {
          properties = new Map<string, number>();
          subjectToProperties.set(subjectKey, properties);
        }

        const predicateKey = RdfString.termToString(predicate);
        properties.set(predicateKey, (properties.get(predicateKey) ?? 0) + 1);
      }
    } catch (err) {
      console.error('Error ingesting quads into cset cache:', err);
      throw err;
    }

    // Next construct the csets by iterating over the grouped subject / predicates and
    // incrementing the relevant counts
    const csetsDocument = new Map<string, ICset>();
    for (const predicates of subjectToProperties.values()) {
      const predicateKeys = [ ...predicates.keys() ];
      const predicateKey = this.serializePredicates(
        predicateKeys
          .map(keyString => <RDF.Quad_Predicate> RdfString.stringToTerm(keyString)),
      );
      let cset = csetsDocument.get(predicateKey);
      if (!cset) {
        cset = {
          subjCount: 0,
          predicateCounts: new Map(predicateKeys.map(key => [ key, 0 ])),
        };
        csetsDocument.set(predicateKey, cset);
      }
      cset.subjCount++;

      for (const [ predKey, count ] of predicates.entries()) {
        const currentTotal = cset.predicateCounts.get(predKey)!;
        cset.predicateCounts.set(predKey, currentTotal + count);
      }
    }
  }

  private serializePredicates(predicates: RDF.Quad_Predicate[]): string {
    return JSON.stringify(
      predicates
        .map(pred => RdfString.termToString(pred))
        .sort(),
    );
  }

  protected onDispose(value: ISourceState, key: string, reason: LRUCache.DisposeReason): void {}

  public async has(key: string): Promise<boolean> {}

  public async delete(key: string): Promise<boolean> {}

  public entries(): AsyncIterator<[string, ISourceState]> {}

  public async size(): Promise<number> {}

  public async serialize(): Promise<void> {}

  public async deserialize(): Promise<void> {}

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

export interface ICset {
  /**
   * Number of occurrences of this cset
   */
  subjCount: number;
  /**
   * Number of occurrences of the predicates within the cset
   */
  predicateCounts: Map<string, number>;
}
