import type { ICompositeExecutionBlock } from '@comunica/bus-derived-resource-partition';

/**
 * Opens once every composite request sent for an execution has had its response.
 * Large triple pattern queries that are covered by composite can slow down the server,
 * causing the composite source response to be delayed. Slowing down the time untill first result.
 */

export class CompositeResponseGate {
  /**
   * Responses underway
   */
  private readonly responses: Promise<unknown>[] = [];
  private readonly accepted: ICompositeExecutionBlock[] = [];
  private closeGate!: () => void;

  private readonly closed = new Promise<void>((resolve) => {
    this.closeGate = resolve;
  });

  /**
   * Adds the response of a composite request that was sent.
   */
  public add(response: Promise<unknown>): void {
    // A failing request has had its response as much as a succeeding one
    this.responses.push(response.catch(() => undefined));
  }

  /**
   * Records a composite block the join took on, which answers its operations for every solution its
   * resource claims.
   */
  public accept(block: ICompositeExecutionBlock): void {
    this.accepted.push(block);
  }

  /**
   * The composite blocks the join took on so far. Complete once the gate is closed.
   */
  public acceptedBlocks(): ICompositeExecutionBlock[] {
    return this.accepted;
  }

  /**
   * Marks that no further composite requests will be added.
   */
  public close(): void {
    this.closeGate();
  }

  /**
   * Resolves once the gate is closed and every added response has arrived.
   */
  public async wait(): Promise<void> {
    await this.closed;
    await Promise.all(this.responses);
  }
}
