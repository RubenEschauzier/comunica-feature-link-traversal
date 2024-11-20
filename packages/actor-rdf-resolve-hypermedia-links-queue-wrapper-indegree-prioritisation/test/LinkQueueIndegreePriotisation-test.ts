import { LinkQueuePriority } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-priority';
import { StatisticLinkDereference } from '@comunica/statistic-link-dereference';
import { StatisticLinkDiscovery } from '@comunica/statistic-link-discovery';
import { StatisticTraversalTopology } from '@comunica/statistic-traversal-topology';
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
import { LinkQueueIndegreePrioritization } from '../lib/LinkQueueIndegreePrioritization';

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

  let statisticDiscovery: StatisticLinkDiscovery;
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
      statisticDiscovery.updateStatistic({ url: 'http://url1/' }, { url: 'http://url2/' });
      queue.push({ url: 'http://url1/' }, { url: 'http://url2/' });
      queue.pop();
      expect(updateIndegreesSpy).toHaveBeenCalledTimes(1);
    });
    it('should be called when calling peek after topology update', () => {
      const updateIndegreesSpy = jest.spyOn(queue, 'updateIndegrees');
      statisticDiscovery.updateStatistic({ url: 'http://url1/' }, { url: 'http://url2/' });
      queue.push({ url: 'http://url1/' }, { url: 'http://url2/' });
      queue.peek();
      expect(updateIndegreesSpy).toHaveBeenCalledTimes(1);
    });

    describe('an example topology', () => {
      let setPrioritySpy;

      beforeEach(() => {
        setPrioritySpy = jest.spyOn(inner, 'setPriority');
        statisticDiscovery.updateStatistic({ url: 'http://B' }, { url: 'http://A' });
        statisticDiscovery.updateStatistic({ url: 'http://C' }, { url: 'http://A' });
        statisticDiscovery.updateStatistic({ url: 'http://C' }, { url: 'http://B' });
        statisticDiscovery.updateStatistic({ url: 'http://D' }, { url: 'http://B' });
        statisticDiscovery.updateStatistic({ url: 'http://D' }, { url: 'http://C' });
        statisticDiscovery.updateStatistic({ url: 'http://C' }, { url: 'http://D' });
        queue.push({ url: 'http://B' }, { url: 'http://A' });
        queue.push({ url: 'http://C' }, { url: 'http://A' });
        queue.push({ url: 'http://C' }, { url: 'http://B' });
        queue.push({ url: 'http://D' }, { url: 'http://B' });
        queue.push({ url: 'http://D' }, { url: 'http://C' });
        queue.push({ url: 'http://C' }, { url: 'http://D' });
      });
      it('should only set priorities for indegree > 1', () => {
        queue.updateIndegrees();
        expect(setPrioritySpy).toHaveBeenNthCalledWith(1, 'http://c/', 3);
        expect(setPrioritySpy).toHaveBeenNthCalledWith(2, 'http://d/', 2);
      });
      it('should only set priorities for open nodes', () => {
        statisticDereference.updateStatistic({ url: 'http://c/' }, new MockQuerySource('URL'));
        queue.updateIndegrees();
        expect(setPrioritySpy).toHaveBeenNthCalledWith(1, 'http://d/', 2);
      });
      it('should correctly set priority on pop', () => {
        const popped = queue.pop();
        expect(popped).toEqual({ url: 'http://c/', metadata: { priority: 3, index: 0 }});
      });
      it('should correctly set priority on peek', () => {
        const peeked = queue.peek();
        expect(peeked).toEqual({ url: 'http://c/', metadata: { priority: 3, index: 0 }});
      });
    });
  });

  describe('push', () => {
    it('should add new links with priority 0', () => {
      mockedQueue.push({ url: 'http://url1/' }, { url: 'http://url2/' });
      expect(mockedInner.push).toHaveBeenCalledWith(
        { url: 'http://url1/', metadata: { priority: 0 }}, { url: 'http://url2/' }
      );
    });
    it('should retain any existing metadata', () => {
      mockedQueue.push({ url: 'http://url1/', metadata: { key: 'value' }}, { url: 'http://url2/' });
      expect(mockedInner.push).toHaveBeenCalledWith(
        { url: 'http://url1/', metadata: { priority: 0, key: 'value' }},
        { url: 'http://url2/' },
      );
    });
  });

  describe('pop', () => {
    it('should not update if no topology updates occured', () => {
      const updateIndegreesSpy = jest.spyOn(queue, 'updateIndegrees');
      queue.push({ url: 'http://url1/' }, { url: 'http://url2/' });
      queue.pop();
      expect(updateIndegreesSpy).not.toHaveBeenCalled();
    });
    it('should not update if the queue is empty', () => {
      const updateIndegreesSpy = jest.spyOn(queue, 'updateIndegrees');
      statisticDiscovery.updateStatistic({ url: 'http://url1/' }, { url: 'http://url2/' });
      queue.pop();
      expect(updateIndegreesSpy).not.toHaveBeenCalled();
    });
  });

  describe('peek', () => {
    it('should not update if no topology updates occured', () => {
      const updateIndegreesSpy = jest.spyOn(queue, 'updateIndegrees');
      queue.push({ url: 'http://url1/' }, { url: 'http://url2/' });
      queue.peek();
      expect(updateIndegreesSpy).not.toHaveBeenCalled();
    });
  });
});
