import type { IJoinEntry } from '@comunica/types';
import type { Operation } from 'sparqlalgebrajs/lib/algebra';
import { FifoQueue } from './FifoQueue';

export class JoinGraph {
  private readonly adjacencyList: Map<number, number[]>;
  private entries: IJoinEntry[];

  public entryToVertex: Map<Operation, number>;
  public vertexToEntry: Map<number, Operation>;
  public size: number;

  constructor(entries: IJoinEntry[]) {
    this.adjacencyList = new Map();
    this.entries = entries;

    this.entryToVertex = new Map();
    this.vertexToEntry = new Map();
    this.size = entries.length;
  }

  public constructJoinGraphBFS(start: IJoinEntry) {
    // First extract all variables in each of the joined triple patterns
    const variablesTriplePatterns: Set<string>[] = [];
    for (const [ i, x ] of this.entries.entries()) {
      this.adjacencyList.set(i, []);
      variablesTriplePatterns.push(this.extractVariables(x.operation));
    }

    const operationToIndex: Map<IJoinEntry, number> = new Map();
    const queue: FifoQueue<IJoinEntry> = new FifoQueue();
    const visited: Map<IJoinEntry, boolean> = new Map();

    let currentVertex = 0;
    let nextAvailableIndex = 1;

    // Start BFS from the startVertex
    queue.enqueue(start);
    visited.set(start, true);

    while (queue.size() > 0) {
      const vertex = queue.dequeue()!;
      operationToIndex.set(vertex, currentVertex);
      // Get all the adjacent vertices
      const neighbors = this.getJoinConnections(vertex, variablesTriplePatterns);
      if (neighbors) {
        for (const neighbor of neighbors) {
          if (!operationToIndex.has(neighbor)) {
            operationToIndex.set(neighbor, nextAvailableIndex);
            nextAvailableIndex += 1;
          }
          this.adjacencyList.get(currentVertex)!.push(operationToIndex.get(neighbor)!);
          // If the neighbor hasn't been visited, visit it
          if (!visited.has(neighbor)) {
            queue.enqueue(neighbor);
            visited.set(neighbor, true);
          }
        }
      }
      currentVertex += 1;
    }
    // Reorder operations according to BFS order
    for (const op of operationToIndex.keys()) {
      this.entries[operationToIndex.get(op)!] = op;
    }
    // Define a mapping from entry to vertex index
  }

  public getJoinConnections(op: IJoinEntry, variablesTriplePatterns: Set<string>[]): IJoinEntry[] {
    const connections: IJoinEntry[] = [];
    for (let i = 0; i < this.entries.length; i++) {
      for (const variable of this.extractVariables(op.operation)) {
        if (variablesTriplePatterns[i].has(variable) && this.entries[i].operation !== op.operation) {
          connections.push(this.entries[i]);
        }
      }
    }
    return connections;
  }

  /**
   * Extracts all variables in triple pattern, currently does not take graph into account,
   * extend by adding 'graph' to the forEached array
   * @param op
   */
  public extractVariables(op: Operation) {
    const variables: Set<string> = new Set();
    for (const type of [ 'subject', 'predicate', 'object' ]) {
      if (op[type].termType == 'Variable') {
        variables.add(op[type].value);
      }
    }
    return variables;
  }

  public getEntries() {
    return this.entries;
  }

  public getAdjencyList() {
    return this.adjacencyList;
  }
}
