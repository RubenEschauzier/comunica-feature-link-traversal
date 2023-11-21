import type { ILinkQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueuePriorityNonAdaptive } from '../lib';

describe('LinkQueueLimitCount', () => {
  let inner: ILinkQueue;
  let queue: LinkQueuePriorityNonAdaptive;

  beforeEach(() => {
    inner = <any> {
      push: jest.fn(() => true),
    };
    queue = new LinkQueuePriorityNonAdaptive(inner);
  });

  describe('push', () => {
    it('invokes the inner queue', () => {
      expect(queue.push({ url: 'a' }, { url: 'parent' })).toBeTruthy();
      expect(inner.push).toHaveBeenCalledWith({ url: 'a' }, { url: 'parent' });
    });
  });
});
