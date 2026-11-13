import { LinkQueueDerivedResourcePrioritization } from '../lib/LinkQueueDerivedResourcePrioritization';

describe('LinkQueueDerivedResourcePrioritization', () => {
  const DERIVED = 'https://knows.idlab.ugent.be/vocab/derived#definedBy';
  const IN_DATASET = 'http://rdfs.org/ns/void#inDataset';
  let pushed: any[];
  let queue: LinkQueueDerivedResourcePrioritization;

  beforeEach(() => {
    pushed = [];
    const inner: any = {
      push: (link: any) => {
        pushed.push(link);
        return true;
      },
      pop: () => pushed.shift(),
      peek: () => pushed[0],
      getSize: () => pushed.length,
      isEmpty: () => pushed.length === 0,
    };
    queue = new LinkQueueDerivedResourcePrioritization(inner, [ DERIVED, IN_DATASET ]);
  });

  function link(url: string, matchingPredicate?: string): any {
    return { url, metadata: matchingPredicate ? { producedByActor: { matchingPredicate }} : undefined };
  }

  it('gives links of every configured predicate the derived resource priority', () => {
    queue.push(link('http://ex.org/pod/', IN_DATASET));
    queue.push(link('http://ex.org/pod/derived', DERIVED));
    expect(pushed.map(l => l.metadata.priority)).toEqual([
      LinkQueueDerivedResourcePrioritization.derivedResourcePriority,
      LinkQueueDerivedResourcePrioritization.derivedResourcePriority,
    ]);
  });

  it('leaves links of other predicates, or without provenance, as they are', () => {
    queue.push(link('http://ex.org/pod/doc', 'http://www.w3.org/ns/ldp#contains'));
    queue.push(link('http://ex.org/pod/other'));
    expect(pushed[0].metadata.priority).toBeUndefined();
    expect(pushed[1].metadata).toBeUndefined();
  });
});
