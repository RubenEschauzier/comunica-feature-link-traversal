export interface ICacheKey<I, S, C> {
  id: string;

  // These don't exist at runtime,
  // but they tell TypeScript what S and C are.
  // Input to the set function under that cache key
  inputType: I;
  // Content of the cache under that key
  setType: S;
  contextType: C;
}

export class CacheKey<I, S, C> implements ICacheKey<I, S, C> {
  public id: string;

  // These don't exist at runtime,
  // but they tell TypeScript what T and C are.
  public inputType!: I;
  public setType!: S;
  public contextType!: C;

  public constructor(id: string) {
    this.id = id;
  }
}
