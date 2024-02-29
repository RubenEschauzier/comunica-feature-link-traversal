import type { IActionConstructTraversedTopology } from '@comunica/bus-construct-traversed-topology';
import { ActionContext, Bus } from '@comunica/core';
import { ActorConstructTraversedTopologyUrlToGraph } from '../lib/ActorConstructTraversedTopologyUrlToGraph';

describe('ActorConstructTraversedTopologyUrlToGraph', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorConstructTraversedTopologyUrlToGraph instance', () => {
    let actor: ActorConstructTraversedTopologyUrlToGraph;
    let parentAction: IActionConstructTraversedTopology;

    beforeEach(() => {
      actor = new ActorConstructTraversedTopologyUrlToGraph({ name: 'actor', bus });
      parentAction =
      {
        parentUrl: 'null',
        foundLinks: [{ url: 'L1' }],
        metadata: [{ sourceNode: true }],
        context: new ActionContext(),
      };
    });
    it('should test', () => {
      return expect(actor.test(parentAction)).resolves.toEqual(true); // TODO
    });

    it('should run', () => {
      return expect(actor.run(parentAction)).resolves.toEqual(true);
    });

    it('should run with parent node', () => {
      const traversalActionA: IActionConstructTraversedTopology = { parentUrl: 'L1',
        foundLinks: [{ url: 'L2' }, { url: 'L3' }, { url: 'L4' }],
        metadata: [{ sourceNode: false }, { sourceNode: false }, { sourceNode: false }],
        context: new ActionContext() };
      actor.run(parentAction);
      return expect(actor.run(traversalActionA)).resolves.toEqual(true);
    });

    it('should not run without defining parent node', () => {
      const traversalActionA: IActionConstructTraversedTopology = { parentUrl: 'L1',
        foundLinks: [{ url: 'L2' }, { url: 'L3' }, { url: 'L4' }],
        metadata: [{ sourceNode: false }, { sourceNode: false }, { sourceNode: false }],
        context: new ActionContext() };
      return expect(actor.run(traversalActionA)).rejects.toThrow(
        new Error('Adding node to traversed graph that has an unknown parent node'),
      );
    });
  });
});

