import { ActionContext } from '@comunica/core';
import type { ISourceState, ICacheMetrics, IPersistentCache } from '@comunica/types';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import type { LRUCache } from 'lru-cache';
import { DataFactory } from 'rdf-data-factory';
import * as RdfString from 'rdf-string';

// eslint-disable-next-line ts/no-require-imports,ts/no-var-requires
const murmurhash = require('murmurhash')

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
    throw new Error("Not yet implemented")
  }

  public getSync(key: string): ISourceState | undefined {
    throw new Error("Not yet implemented")
  }

  public async getMany(keys: string[]): Promise<(ISourceState | undefined)[]> {
    throw new Error("Not yet implemented")
  }

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
    const subjectData = new Map<string, {
      properties: Map<string, number>
      objects: Map<string, Set<number>>
    }>();
    try {
      for await (const quad of quads) {
        const { subject, predicate, object } = quad;

        const subjectKey = this.serializeTerm(subject);

        let data = subjectData.get(subjectKey);
        if (!data) {
          data = { properties: new Map(), objects: new Map() };
          subjectData.set(subjectKey, data);
        }

        const predicateKey = RdfString.termToString(predicate);
        data.properties.set(predicateKey, (data.properties.get(predicateKey) ?? 0) + 1);     

        if (object.termType === 'NamedNode'|| 
          object.termType === 'BlankNode' || 
          object.termType === 'Quad'
        ){          
          let objectsForPredicate = data.objects.get(predicateKey);
          
          if (!objectsForPredicate) {
            objectsForPredicate = new Set<number>();
            data.objects.set(predicateKey, objectsForPredicate);
          }
          
          objectsForPredicate.add(this.hashTerm(this.serializeTerm(object)));        
        }
      }
    } catch (err) {
      console.error('Error ingesting quads into cset cache:', err);
      throw err;
    }

    // Next construct the csets, cps, localObjects, and localSubjects
    // by iterating over the grouped subject / predicates and
    // incrementing the relevant counts
    const csetsDocument = new Map<string, ICharacteristicSet>();
    for (const [subjectKey, data] of subjectData.entries()) {
      const predicates = data.properties;
      const objects = data.objects;

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
          localSubjects: new Set(),
          localObjects: new Map(predicateKeys.map(key => [ key, new Set() ])),
        };
        csetsDocument.set(predicateKey, cset);
      }
      cset.subjCount++;
      cset.localSubjects.add(this.hashTerm(subjectKey));

      for (const [ predKey, count ] of predicates.entries()) {
        const currentTotal = cset.predicateCounts.get(predKey)!;
        cset.predicateCounts.set(predKey, currentTotal + count);
        const objectsForPredicate = objects.get(predKey);
        if (objectsForPredicate) {
          const targetSet = cset.localObjects.get(predKey)!;
          for (const objectHash of objectsForPredicate){
             targetSet.add(objectHash);
          }
        }
      }
    }

    const entityResolutionMap = new Map<number, string>();
    // Map subjects to their characteristic sets
    for (const [csetKey, cset] of csetsDocument.entries()) {
      for (const subjectHash of cset.localSubjects) {
        entityResolutionMap.set(subjectHash, csetKey);
      }
    }
    
    const localCps: Map<string, ICharacteristicPair> = new Map();
    for (const [subjectCSetKey, cset] of csetsDocument.entries()) {
      for (const [predicateKey, objectHashes] of cset.localObjects.entries()) {
        for (const objectHash of objectHashes) {
          const objectCSetKey = entityResolutionMap.get(objectHash);
          // If we match an entity in the subjects with one of the objects in current
          // cs we have need to update this cp
          if (objectCSetKey) {
            const cpKey = this.toCpKey(subjectCSetKey, predicateKey, objectCSetKey);
            let cp = localCps.get(cpKey);
            if (!cp){
              cp = {
                csetSubj: cset,
                csetObj: csetsDocument.get(objectCSetKey)!,
                predicate: predicateKey,
                count: 0
              }
              localCps.set(cpKey, cp);
            }
            cp.count++;
          }
        }
      }
    }
  }
  private toCpKey(subjKey: string, predicateKey: string, objectKey: string){
    return `${subjKey}|${predicateKey}|${objectKey}`;
  }
  
  private hashTerm(term: string): number{
    return murmurhash.v3(term);
  }

  private serializeTerm(term: RDF.Term): string {
    return term.termType === 'Quad'
      ? JSON.stringify(RdfString.quadToStringQuad(term))
      : RdfString.termToString(term);
  }
  private serializePredicates(predicates: RDF.Quad_Predicate[]): string {
    return JSON.stringify(
      predicates
        .map(pred => RdfString.termToString(pred))
        .sort(),
    );
  }

  protected onDispose(value: ISourceState, key: string, reason: LRUCache.DisposeReason): void {}

  public async has(key: string): Promise<boolean> {
    throw new Error("Not yet implemented")
  }

  public async delete(key: string): Promise<boolean> {
    throw new Error("Not yet implemented")
  }

  public entries(): AsyncIterator<[string, ISourceState]> {
    throw new Error("Not yet implemented")
  }

  public async size(): Promise<number> {
    throw new Error("Not yet implemented")
  }

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
/**
 * Interface for cset. Will also use this for global csets by keeping
 * track of number of contributing values one document does.
 * Then on dispose decrement relevant values
 */
export interface ICharacteristicSet {
  /**
   * Number of occurrences of this cset
   */
  subjCount: number;
  /**
   * Number of occurrences of the predicates within the cset
   */
  predicateCounts: Map<string, number>;
  /**
   * Hashed entity set of this cSet
   */
  localSubjects: Set<number>;
  /**
   * Hashed objects set of this cSet for each predicate in the cset
   */
  localObjects: Map<string, Set<number>>;
}

/**
 * Same principle as cset for global statistics
 */
export interface ICharacteristicPair {
  /**
   * The cset of the subject of the pair
   */
  csetSubj: ICharacteristicSet;
  /**
   * The cset of the object of the pair
   */
  csetObj: ICharacteristicSet;
  /**
   * The predicate connecting the pairs (check: is hash?)
   */
  predicate: string;
  /**
   * Number of these connections
   */
  count: number;
}