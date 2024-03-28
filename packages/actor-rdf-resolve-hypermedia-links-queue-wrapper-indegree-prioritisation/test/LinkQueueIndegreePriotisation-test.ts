import type { ILinkQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueIndegreePrioritisation } from '..';
import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { AdjacencyListGraph } from '../../actor-construct-traversed-topology-graph-based-prioritisation/lib/AdjacencyListGraph';


describe('LinkQueueIndegreePrioritisation', () => {
  let inner: LinkQueuePriority;
  let queue: LinkQueueIndegreePrioritisation;

  let mockedInner: LinkQueuePriority;
  let mockedQueue: LinkQueueIndegreePrioritisation;

  let graph: AdjacencyListGraph

  beforeEach(() => {
    graph = new AdjacencyListGraph()

    inner = new LinkQueuePriority()
    queue = new LinkQueueIndegreePrioritisation(inner, graph);

    mockedInner  = <any> {
        push: jest.fn(() => true),
    };
  
    mockedQueue = new LinkQueueIndegreePrioritisation(mockedInner, graph);
  });

  describe('Static graph', () => {
    beforeEach(() => {
        graph.set('A', '', {});
        graph.set('B', 'A', {});
        graph.set('C', 'A', {});
        graph.set('D', 'A', {});
        graph.set('D', 'B', {});
        graph.set('E', 'A', {});
        graph.set('E', 'B', {});
        graph.set('E', 'C', {});    
    });

    it('invokes the inner queue', () => {
        expect(mockedQueue.push({ url: 'A' }, { url: '' })).toBeTruthy();
        expect(mockedInner.push).toHaveBeenCalledWith(
          { url: 'A', priority: 0}, {url: ''},
        );
    });
    
    it('push and pops single links', () => {
      expect(queue.push({ url: 'a' }, { url: 'parent' })).toBeTruthy();
      expect(queue.pop()).toEqual( {url: 'a', priority: 0, index: 0})
    });

    it('push and pops single links for empty metadatas', () => {
      expect(queue.push({ url: 'a', metadata: {}}, { url: 'parent', metadata: {}})).toBeTruthy();
      expect(queue.pop()).toEqual( {url: 'a', metadata: {}, priority: 0, index: 0})
    });

    it('assigns proper priority for URL in graph', () => {
        expect(queue.push({ url: 'A' }, { url: '' })).toBeTruthy();
        expect(queue.push({ url: 'B' }, { url: 'A' })).toBeTruthy();
        expect(queue.push({ url: 'C' }, { url: 'A' })).toBeTruthy();
        expect(queue.push({ url: 'D' }, { url: 'A' })).toBeTruthy();
        expect(queue.push({ url: 'E' }, { url: 'A' })).toBeTruthy();

        expect(queue.pop()).toEqual( {url: 'E', priority: 3, index: 0})
        expect(queue.pop()).toEqual( {url: 'D', priority: 2, index: 0})
        // While in this queue the order of equal priority elements is not guaranteed, it should in this case be B
        expect(queue.pop()).toEqual( {url: 'B', priority: 0, index: 0})
    });
    it('only calls updatePriority on pop when indegree has changed', () => {
        const spy = jest.spyOn(inner, 'updatePriority');
        queue.push({ url: 'A' }, { url: '' });
        queue.push({ url: 'B' }, { url: 'A' });
        queue.push({ url: 'C' }, { url: 'A' });
        queue.push({ url: 'D' }, { url: 'A' });
        queue.push({ url: 'E' }, { url: 'A' });
        queue.pop();
        expect(spy.mock.calls).toEqual([["D", 2], ["E", 3]]);
    })
  });
  describe('Changing graph', () => {
    beforeEach(() => {
        graph.set('A', '', {});
        graph.set('B', 'A', {});
        graph.set('C', 'A', {});
        graph.set('D', 'A', {});
        graph.set('D', 'B', {});
        queue.push({ url: 'A' }, { url: '' });
        queue.push({ url: 'B' }, { url: 'A' });
        queue.push({ url: 'C' }, { url: 'A' });
        queue.push({ url: 'D' }, { url: 'A' });

    })
    it('assign correct priority to new link on peek and pop', () => {
        graph.set('E', 'A', {});
        graph.set('E', 'B', {});
        graph.set('E', 'C', {});

        queue.push({url: 'E'}, {url: 'A'});
        expect(queue.pop()).toEqual({url: 'E', priority: 3, index: 0});
        expect(queue.peek()).toEqual({url: 'D', priority: 2, index: 0});
        graph.set('F', 'B', {});
        queue.push({url: 'F'}, {url: 'B'});
        expect(queue.peek()).toEqual({url: 'D', priority: 2, index: 0});
        graph.set('F', 'A', {});
        graph.set('F', 'C', {});
        graph.set('F', 'D', {});
        expect(queue.peek()).toEqual({url: 'F', priority: 4, index: 0});
        expect(queue.pop()).toEqual({url: 'F', priority: 4, index: 0});
    })
  })
});
