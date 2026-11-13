import { LinkQueueFifo } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-fifo';
import { LinkQueueWrapperKnownSources } from '../lib/LinkQueueWrapperKnownSources';

describe('LinkQueueWrapperKnownSources', () => {
  let queue: LinkQueueWrapperKnownSources;

  beforeEach(() => {
    queue = new LinkQueueWrapperKnownSources(new LinkQueueFifo(), [ 'http://ex.org/shop/', 'http://ex.org/reviews/' ]);
  });

  it('refuses links into a known namespace, fragments included', () => {
    expect(queue.push({ url: 'http://ex.org/shop/product1' })).toBe(false);
    expect(queue.push({ url: 'http://ex.org/reviews/r1#it' }, { url: 'http://ex.org/other' })).toBe(false);
    expect(queue.isEmpty()).toBe(true);
  });

  it('accepts other links, the known sources themselves included', () => {
    expect(queue.push({ url: 'http://ex.org:8080/shop/sparql' })).toBe(true);
    expect(queue.push({ url: 'http://ex.org/shopping/list' })).toBe(true);
    expect(queue.pop()).toEqual({ url: 'http://ex.org:8080/shop/sparql' });
    expect(queue.pop()).toEqual({ url: 'http://ex.org/shopping/list' });
  });
});
