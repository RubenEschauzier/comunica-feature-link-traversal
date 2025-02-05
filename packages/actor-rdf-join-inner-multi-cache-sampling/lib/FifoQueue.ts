export class FifoQueue<T> {
  private storage: T[] = [];
  private minSize: number = 0;
  // Add an element to the back of the queue
  enqueue(item: T): void {
    this.storage.push(item);
  }

  // Remove an element from the front of the queue
  dequeue(): T | undefined {
    return this.storage.shift(); // Removes the first element in the array
  }

  // Peek at the front element without removing it
  peek(): T | undefined {
    return this.storage[0];
  }

  // Check if the queue is empty
  isEmpty(): boolean {
    return this.storage.length === 0;
  }

  // Get the number of elements in the queue
  size(): number {
    return this.storage.length;
  }

  // Clear the queue
  clear(): void {
    this.storage = [];
  }
}
