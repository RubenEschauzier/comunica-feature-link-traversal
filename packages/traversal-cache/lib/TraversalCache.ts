import { Algebra } from "sparqlalgebrajs";
import { LRUCache } from 'lru-cache';
import { ITraversalIndex, TraversalCacheIndex } from "./TraversalCacheIndex";

export class TraversalCache<K extends string, V extends {}> implements ITraversalCache<K, V>{
    private cache: LRUCache<K, V>;
    private traversalIndex: ITraversalIndex<K>;
    private solidTraversalPredicates: string[];

    public constructor(maxSize: number){
        this.traversalIndex = new TraversalCacheIndex<K>();
        this.cache = new LRUCache({ maxSize, dispose: (value, key) => this.traversalIndex.delete(key) });

        // TODO: Add solid traversal predicates
        this.solidTraversalPredicates = [];
    }

    public set(key: K, value: V): void {
        this.cache.set(key, value);
        // Add traversalIndex set logic here
    }

    public get(key: K, reachableSources?: Set<K> | undefined): V | undefined {
        if (!reachableSources?.has(key)){
            return undefined;
        }
        return this.cache.get(key);
    }

    public clear(): void {
        this.cache.clear();
        this.traversalIndex.clear()
    }

    public has(key: K, reachableSources: Set<K>): boolean {
        if (!reachableSources?.has(key)){
            return false;
        }
        return this.cache.has(key);
    }

    /**
     * Finds all sources reachable using cMatch and solid predicates.
     * @param query query algebra
     */
    public findRelevant(query: Algebra.Operation): Set<K> {
        // Extract predicates from query
        throw new Error("Method not implemented.");
    }

}

export interface ITraversalCache<K extends {}, V extends {}>{
    /**
     * Inserts a key-value pair into the cache.
     * @param key The key associated with the data source (e.g., query or resource identifier).
     * @param value The data source or its metadata.
     */
    set(key: K, value: V): void;
    
    /**
     * Retrieves the data source associated with a key. Takes an optional list of keys
     * that are accessible. This value is computed for a given query by the findReachable
     * function.
     * @param key The key to search for.
     * @returns The associated data source, or undefined if not found.
     */
    get(key: K, reachableSources?: Set<K>): V | undefined;
        
    /**
     * Clears the cache.
     */
    clear(): void;
    
    /**
     * Checks if a key exists in the cache. Takes an optional list of keys
     * that are accessible.
     * @param key The key to check.
     * @returns True if the key exists, false otherwise.
     */
    has(key: K, reachableSources?: Set<K>): boolean;

    /**
     * Finds accessible keys in the cache relevant to a given query. This can be expensive
     * so it is best to reuse this for each query.
     * @param query The query to match against the index.
     * @returns A list of accessible data sources or keys.
     */
    findRelevant(query: Algebra.Operation): Set<K>;
}

