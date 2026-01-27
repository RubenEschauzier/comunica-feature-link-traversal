export interface IViewKey<S, C, K> {
  id: string;

  // These don't exist at runtime,
  // but they tell TypeScript what T and C are.
  inputType: S;
  outputType: K;
  contextType: C;
}

/**
 * Identifies a specific way to read data.
 * T: What this view returns (e.g. ISourceState)
 * C: What context this view needs (e.g. { url: string })
 * K:  What this view expects the cache to contain (MUST match CacheKey<TStored>)
 */
export class ViewKey<S, C, K> implements IViewKey<S, C, K> {
  public id: string;

  // These don't exist at runtime,
  // but they tell TypeScript what T and C are.
  public inputType!: S;
  public outputType!: K;
  public contextType!: C;

  constructor(id: string) {
    this.id = id;
  }
}
