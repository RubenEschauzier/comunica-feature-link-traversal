import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { LinkQueueIndegreePrioritization } from '../lib/LinkQueueIndegreePrioritization';
import { StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';
import { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import type {
  BindingsStream,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
  QuerySourceReference,
} from '@comunica/types';
import type { Quad } from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import type { Operation, Ask, Update } from 'sparqlalgebrajs/lib/algebra';

class MockQuerySource implements IQuerySource {
  public referenceValue: QuerySourceReference;

  public constructor(referenceValue: QuerySourceReference) {
    this.referenceValue = referenceValue;
  }

  public getSelectorShape: (context: IActionContext) => Promise<FragmentSelectorShape>;
  public queryBindings: (operation: Operation, context: IActionContext, options?: IQueryBindingsOptions | undefined)
  => BindingsStream;

  public queryQuads: (operation: Operation, context: IActionContext) => AsyncIterator<Quad>;
  public queryBoolean: (operation: Ask, context: IActionContext) => Promise<boolean>;
  public queryVoid: (operation: Update, context: IActionContext) => Promise<void>;
  public toString: () => string;
}


describe('LinkQueueIndegreePrioritisation', () => {
  let inner: LinkQueuePriority;
  let queue: LinkQueueIndegreePrioritization;
  let mockedInner: LinkQueuePriority;
  let mockedQueue: LinkQueueIndegreePrioritization;

  let statisticDiscovery: StatisticLinkDiscovery
  let statisticDereference: StatisticLinkDereference;
  let statisticTraversalTopology: StatisticTraversalTopology;

  beforeEach(() => {
    statisticDiscovery = new StatisticLinkDiscovery();
    statisticDereference = new StatisticLinkDereference();
    statisticTraversalTopology = 
      new StatisticTraversalTopology(statisticDiscovery, statisticDereference);

    inner = new LinkQueuePriority();
    queue = new LinkQueueIndegreePrioritization(inner, statisticTraversalTopology);

    mockedInner = <any> {
      push: jest.fn(() => true),
      
    };
    mockedQueue = new LinkQueueIndegreePrioritization(mockedInner, statisticTraversalTopology);
  });

  describe('updateIndegrees', () => {

    it('should be called when calling pop after topology update', () => {
      const updateIndegreesSpy = jest.spyOn(queue, 'updateIndegrees');
      statisticDiscovery.updateStatistic({url: 'url1'}, {url: 'url2'});
      queue.push({url: 'url1'}, {url: 'url2'})
      queue.pop();
      expect(updateIndegreesSpy).toHaveBeenCalledTimes(1);
    });
    it('should be called when calling peek after topology update', () => {
      const updateIndegreesSpy = jest.spyOn(queue, 'updateIndegrees');
      statisticDiscovery.updateStatistic({url: 'url1'}, {url: 'url2'});
      queue.push({url: 'url1'}, {url: 'url2'})
      queue.peek();
      expect(updateIndegreesSpy).toHaveBeenCalledTimes(1);
    });

    describe('an example topology', () => {
      let setPrioritySpy;

      beforeEach(() => {
        setPrioritySpy = jest.spyOn(inner, 'setPriority');
        statisticDiscovery.updateStatistic({url: 'B'}, {url: 'A'});
        statisticDiscovery.updateStatistic({url: 'C'}, {url: 'A'});
        statisticDiscovery.updateStatistic({url: 'C'}, {url: 'B'});
        statisticDiscovery.updateStatistic({url: 'D'}, {url: 'B'});
        statisticDiscovery.updateStatistic({url: 'D'}, {url: 'C'});
        statisticDiscovery.updateStatistic({url: 'C'}, {url: 'D'});
        queue.push({url: 'B'}, {url: 'A'});
        queue.push({url: 'C'}, {url: 'A'});
        queue.push({url: 'C'}, {url: 'B'});
        queue.push({url: 'D'}, {url: 'B'});
        queue.push({url: 'D'}, {url: 'C'});
        queue.push({url: 'C'}, {url: 'D'});

      })
      it('should only set priorities for indegree > 1', () => {
        queue.updateIndegrees();
        expect(setPrioritySpy).toHaveBeenNthCalledWith(1, 'C', 3);
        expect(setPrioritySpy).toHaveBeenNthCalledWith(2, 'D', 2)
      });
      it('should only set priorities for open nodes', () => {
        statisticDereference.updateStatistic({url: 'C'}, new MockQuerySource('URL'));
        queue.updateIndegrees();
        expect(setPrioritySpy).toHaveBeenNthCalledWith(1, 'D', 2)
      });
      it('should correctly set priority on pop', () => {
        const popped = queue.pop();
        expect(popped).toEqual({url: 'C', metadata: {priority: 3, index: 0}});
      });
      it('should correctly set priority on peek', () => {
        const peeked = queue.peek();
        expect(peeked).toEqual({url: 'C', metadata: {priority: 3, index: 0}});
      });
    })
  });

  describe('push', () => {
    it('should add new links with priority 0', () => {
      mockedQueue.push({url: 'url1'}, {url: 'url2'});
      expect(mockedInner.push).toHaveBeenCalledWith({url: 'url1', metadata: {priority: 0}},
        {url: 'url2'}
      );
    });
    it('should retain any existing metadata', () => {
      mockedQueue.push({url: 'url1', metadata: {key: 'value'}}, {url: 'url2'});
      expect(mockedInner.push).toHaveBeenCalledWith(
        { url: 'url1', metadata: {priority: 0, key: 'value'}},
        {url: 'url2'}
      );
    });  
  });

  describe('pop', () => {
    it('should not update if no topology updates occured', () => {
      const updateIndegreesSpy = jest.spyOn(queue, 'updateIndegrees');
      queue.push({url: 'url1'}, {url: 'url2'});
      queue.pop();
      expect(updateIndegreesSpy).not.toHaveBeenCalled();
    });
    it('should not update if the queue is empty', () => {
      const updateIndegreesSpy = jest.spyOn(queue, 'updateIndegrees');
      statisticDiscovery.updateStatistic({url: 'url1'}, {url: 'url2'});
      queue.pop();
      expect(updateIndegreesSpy).not.toHaveBeenCalled();
    });  
  });

  describe('peek', () => {
    it('should not update if no topology updates occured', () => {
      const updateIndegreesSpy = jest.spyOn(queue, 'updateIndegrees');
      queue.push({url: 'url1'}, {url: 'url2'});
      queue.peek();
      expect(updateIndegreesSpy).not.toHaveBeenCalled();
    });  
  })
});
