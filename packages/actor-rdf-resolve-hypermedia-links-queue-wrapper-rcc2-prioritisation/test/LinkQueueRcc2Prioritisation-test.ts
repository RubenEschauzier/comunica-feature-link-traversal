import type { ILinkQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { LinkQueueRcc2Prioritisation } from '..';
import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { AdjacencyListGraphRcc } from '@comunica/actor-construct-traversed-topology-graph-based-prioritisation';


describe('LinkQueueIndegreePrioritisation', () => {
  let inner: LinkQueuePriority;
  let queue: LinkQueueRcc2Prioritisation;

  let mockedInner: LinkQueuePriority;
  let mockedQueue: LinkQueueRcc2Prioritisation;

  let graph: AdjacencyListGraphRcc

  beforeEach(() => {
    graph = new AdjacencyListGraphRcc()

    inner = new LinkQueuePriority()
    queue = new LinkQueueRcc2Prioritisation(inner, graph);

    mockedInner  = <any> {
        push: jest.fn(() => true),
        isEmpty: jest.fn(() => true)
    };
  
    mockedQueue = new LinkQueueRcc2Prioritisation(mockedInner, graph);
  });

  describe('Static graph', () => {
    beforeEach(() => {
        graph.set('A', '', {});
        graph.set('B', 'A', {});
        graph.set('C', 'A', {});
        graph.set('D', 'A', {});
        graph.set('E', 'A', {});

        graph.set('F', 'B', {});
        graph.set('F', 'C', {});        
        
        graph.set('G', 'D', {});
        graph.set('G', 'E', {});
        
        graph.increaseRcc('B', 2);
        graph.increaseRcc('A', 3);
        graph.increaseRcc('C', 2);
        graph.increaseRcc('D', 2);
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
        expect(queue.push({ url: 'E' }, { url: 'A' })).toBeTruthy();
        expect(queue.push({ url: 'F' }, { url: 'B' })).toBeTruthy();
        expect(queue.push({ url: 'G' }, { url: 'D' })).toBeTruthy();

        
        expect(queue.pop()).toEqual( {url: 'F', priority: 7, index: 0});
        expect(queue.pop()).toEqual( {url: 'G', priority: 5, index: 0});
        expect(queue.pop()).toEqual( {url: 'E', priority: 3, index: 0});
        
    });

    it('only calls updatePriority on pop when rcc has changed', () => {
        const spy = jest.spyOn(inner, 'updatePriority');
        queue.push({ url: 'E' }, { url: 'A' });
        queue.push({ url: 'F' }, { url: 'B' });
        queue.push({ url: 'G' }, { url: 'D' });
        queue.pop();
        expect(spy.mock.calls).toEqual([["F", 7], ["B", 3], ["C", 3], ["D", 3], ["G", 5], ["E", 3]]);
    })
  });
  describe('Changing graph', () => {
    beforeEach(() => {
        graph.set('A', '', {});
        graph.set('B', 'A', {});
        graph.set('C', 'A', {});
        graph.set('D', 'A', {});
        graph.set('D', 'B', {});
        graph.increaseRcc('B', 2)
        graph.increaseRcc('C', 3);
        queue.push({ url: 'D' }, { url: 'A' });


    })
    it('assign correct priority to new link on peek and pop', () => {
        graph.set('E', 'A', {});
        graph.set('E', 'B', {});
        graph.set('E', 'C', {});
        queue.push({url: 'E'}, {url: 'A'});

        expect(queue.pop()).toEqual({url: 'E', priority: 5, index: 0});
        expect(queue.peek()).toEqual({url: 'D', priority: 2, index: 0});
        graph.set('F', 'B', {});
        queue.push({url: 'F'}, {url: 'B'})
        graph.set('F', 'C', {});        
        graph.increaseRcc('A', 4);
        graph.increaseRcc('B', 1)
        expect(queue.pop()).toEqual({url: 'F', priority: 10, index: 0});
        expect(queue.peek()).toEqual({url: 'D', priority: 7, index: 0});
    });
    it('assigns correct priority when rcc change between pop and peek', () => {
        graph.set('E', 'A', {});
        queue.push({url: 'E'}, {url: 'A'});
        graph.set('E', 'B', {});
        graph.set('E', 'C', {});
        graph.set('F', 'B', {});
        queue.push({url: 'F'}, {'url': 'B'});
        graph.set('F', 'C', {})
        graph.increaseRcc('A', 4);
        expect(queue.pop()).toEqual({url: 'E', priority: 9, index: 0});
        graph.increaseRcc('C', 10);
        expect(queue.peek()).toEqual({url: 'F', priority: 19, index: 0});
    });
  });
});
