import type { ILink, ILinkQueue } from '@comunica/types';
import type { IDynamicFilter } from '@comunica/types-link-traversal';
import { LinkQueueWrapperFilterDynamic } from '../lib/LinkQueueWrapperFilterDynamic';

function arrayQueue(): ILinkQueue {
  const links: ILink[] = [];
  return <ILinkQueue> <unknown> {
    push: (link: ILink) => links.push(link) > 0,
    pop: () => links.shift(),
    peek: () => links[0],
    getSize: () => links.length,
    isEmpty: () => links.length === 0,
  };
}

function dynamicFilter(excluded: string[] = []): IDynamicFilter {
  return <IDynamicFilter> <unknown> {
    matchesFilter: (url: string) => excluded.includes(url),
    isHeld: () => false,
    addReleaseListener: jest.fn(),
  };
}

describe('LinkQueueWrapperFilterDynamic', () => {
  it('drops links matching the dynamic filter', () => {
    const queue = new LinkQueueWrapperFilterDynamic(arrayQueue(), dynamicFilter([ 'http://a/1' ]));
    queue.push({ url: 'http://a/1' });
    queue.push({ url: 'http://a/2' });
    expect(queue.pop()).toEqual({ url: 'http://a/2' });
    expect(queue.pop()).toBeUndefined();
  });

  it('drops links rejected by a static link filter', () => {
    const queue = new LinkQueueWrapperFilterDynamic(
      arrayQueue(),
      dynamicFilter(),
      [ link => !link.url.includes('/filters') ],
    );
    queue.push({ url: 'http://pod/filters/filter-star-template-1' });
    queue.push({ url: 'http://pod/profile/card' });
    expect(queue.pop()).toEqual({ url: 'http://pod/profile/card' });
    expect(queue.pop()).toBeUndefined();
  });

  it('keeps every link without static link filters', () => {
    const queue = new LinkQueueWrapperFilterDynamic(arrayQueue(), dynamicFilter());
    queue.push({ url: 'http://pod/filters/filter-star-template-1' });
    expect(queue.pop()).toEqual({ url: 'http://pod/filters/filter-star-template-1' });
  });
});
