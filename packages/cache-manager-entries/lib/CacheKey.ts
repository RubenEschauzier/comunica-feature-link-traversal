export interface ICacheKey<T, C> {
  id: string;

  // These don't exist at runtime,
  // but they tell TypeScript what T and C are.
  valueType: T;
  contextType: C;
}

export class CacheKey<T, C> implements ICacheKey<T, C> {
  public id: string;

  // These don't exist at runtime,
  // but they tell TypeScript what T and C are.
  public valueType!: T;
  public contextType!: C;

  public constructor(id: string) {
    this.id = id;
  }
}
