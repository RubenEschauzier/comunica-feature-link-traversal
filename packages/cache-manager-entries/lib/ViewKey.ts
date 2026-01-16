export interface IViewKey<T, C, K> {
  id: string;

  // These don't exist at runtime,
  // but they tell TypeScript what T and C are.
  inputType: T;
  outputType: K;
  contextType: C;
}


/**
 * Identifies a specific way to read data.
 * T: What this view returns (e.g. ISourceState)
 * C: What context this view needs (e.g. { url: string })
 * K:  What this view expects the cache to contain (MUST match CacheKey<TStored>)
 */
export class ViewKey<T, C, K> implements IViewKey<T, C, K> {
  public id: string;

  // These don't exist at runtime,
  // but they tell TypeScript what T and C are.
  public inputType!: T;
  public outputType!: K;
  public contextType!: C;

  constructor(id: string) {
    this.id = id;
  }
}