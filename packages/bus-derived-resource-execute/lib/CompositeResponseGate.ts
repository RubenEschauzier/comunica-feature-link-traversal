/**
 * Opens once every composite request sent for an execution has had its response.
 *
 * A derived resource is evaluated by the server rather than read from disk, and a server evaluating
 * a small composite alongside many pod-wide triple pattern requests answers the composite last: on
 * SolidBench, one composite took ~15ms alone and ~125ms next to the triple pattern requests of the
 * same pod, while sending those 5ms after it brought it back to ~9ms at no cost to them. Requests
 * that nothing is waiting on can therefore wait for this gate, so the composite that produces the
 * first answer is not queued behind them.
 *
 * The composite actor adds its responses and closes the gate once it has sent every request; an
 * execution without composite blocks closes it straight away.
 */
export class CompositeResponseGate {
  private readonly responses: Promise<unknown>[] = [];
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
