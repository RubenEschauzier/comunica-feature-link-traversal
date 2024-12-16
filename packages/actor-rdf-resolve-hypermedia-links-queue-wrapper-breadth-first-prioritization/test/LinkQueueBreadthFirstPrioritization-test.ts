import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import type { ILinkQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueBreadthFirstPrioritization } from '../lib';

describe('LinkQueuePriorityBreadthFirst', () => {
  let inner: ILinkQueue;
  let queue: LinkQueueBreadthFirstPrioritization;

  beforeEach(() => {
    inner = new LinkQueuePriority();
    queue = new LinkQueueBreadthFirstPrioritization(inner);
  });

  describe('push', () => {
    it('assigns correct priority', () => {
      expect(queue.push({ url: 'parent' }, { url: '' })).toBeTruthy();
      expect(queue.push({ url: 'a' }, { url: 'parent' })).toBeTruthy();
      expect(queue.pop()).toEqual({ url: 'parent', metadata: { index: 0, priority: 0 }});
      expect(queue.pop()).toEqual({ url: 'a', metadata: { index: 0, priority: -1 }});
    });
  });
});