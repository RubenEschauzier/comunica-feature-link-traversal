import { ISourceState } from "@comunica/types";
import { IPersistentCache } from "@comunica/types-link-traversal";
import { ArrayIterator, AsyncIterator } from "asynciterator";
import { LRUCache } from "lru-cache";


export class PersistentCacheSourceStateNumTriples implements IPersistentCache<ISourceState> {

    private lruCacheDocuments: LRUCache<string, ISourceState>;

    public constructor(args: IPersistentCacheSourceStateNumTriplesArgs){
        this.lruCacheDocuments = new LRUCache<string, ISourceState>({
            maxSize: args.maxNumTriples,
            sizeCalculation: this.getSizeSource,
        });

    }
    public async get(key: string): Promise<ISourceState | undefined>{
        return this.lruCacheDocuments.get(key);
    }

    public async getMany(keys: string[]): Promise<(ISourceState | undefined)[]>{
        return keys.map(key => this.lruCacheDocuments.get(key));
    }

    public async set(key: string, value: ISourceState): Promise<void>{
        this.lruCacheDocuments.set(key, value);
    }
    public async has(key: string): Promise<boolean>{
        return this.lruCacheDocuments.has(key);
    }

    public async delete(key: string): Promise<boolean>{
        return this.lruCacheDocuments.delete(key);
    }

    public entries(): AsyncIterator<[string, ISourceState]>{
        return new ArrayIterator(
          this.lruCacheDocuments.entries(),
          { autoStart: false }
        );
    }

    public serialize():Promise<void>{
        throw new Error("Serialize implemented for this in-memory cache");
    }

    /**
     * TODO: Implement here that the sourceSize will be handled with a callback
     * once importing the data is done.
     * @returns 
     */
    private getSizeSource(sourceState: ISourceState): number {
        if ('getSize' in sourceState.source 
            && typeof sourceState.source['getSize'] === 'function' ){
            return sourceState.source.getSize();
        }
        return 1;
    }
}

export interface IPersistentCacheSourceStateNumTriplesArgs {
    maxNumTriples: number;
}