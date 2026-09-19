// eslint-disable-next-line import/no-nodejs-modules
import type { EventEmitter } from 'node:events';
import type {
  MetadataQuads,
  MetadataBindings,
  IQuerySource,
  ComunicaDataFactory,
  IActionContext,
} from '@comunica/types';
import type { IAggregatedStore } from '@comunica/types-link-traversal';
import { AlgebraFactory } from '@comunica/utils-algebra';
import { ClosableTransformIterator } from '@comunica/utils-iterator';
import { MetadataValidationState } from '@comunica/utils-metadata';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { wrap } from 'asynciterator';
import { StreamingStore } from 'rdf-streaming-store';

/**
 * Separates the parts of an object key. An object is keyed by its value, so a literal has to be
 * told apart from an IRI reading the same, and two literals differing only in type or language
 * have to be told apart from each other.
 */
const OBJECT_KEY_SEPARATOR = String.fromCodePoint(1);

/**
 * The value the N3 entity index counter is advanced to before any quad is stored.
 * This is to prevent arrays being allocated for single value, inflating memory size
 */
const ENTITY_ID_OFFSET = 1_000_000;

/**
 * A quad carrying the documents that asserted it, as attached by `match`.
 */
export interface IQuadWithSources extends RDF.Quad {
  sources?: string | string[];
}

/**
 * An aggregated store that returns AsyncIterators with a valid MetadataQuads property.
 */
export class AggregatedStoreMemory extends StreamingStore implements IAggregatedStore {
  public started = false;
  public containedSources = new Set<string>();
  public readonly runningIterators: Set<AsyncIterator<RDF.Quad>> = new Set<AsyncIterator<RDF.Quad>>();
  protected readonly iteratorCreatedListeners: Set<() => void> = new Set();
  protected readonly allIteratorsClosedListeners: Set<() => void> = new Set();
  protected readonly metadataAccumulator:
  (accumulatedMetadata: MetadataBindings, appendingMetadata: MetadataBindings) => Promise<MetadataBindings>;

  protected readonly dataFactory: ComunicaDataFactory;

  /**
   * The documents that asserted each triple, nested subject / predicate / object.
   * For triples with different sources (base source + derived resource) this would cause 
   * duplications if stored in graph term.
   */
  protected readonly sourceIndex = new Map<string, Map<string, Map<string, string | string[]>>>();

  /**
   * Whether to record the document each quad was read from.
   */
  protected readonly trackSources: boolean;

  /**
   * Whether the AggregatedStoreMemory should emit updated partial cardinalities
   * for each matching quad. Enabling this option may impact performance due to
   * frequent {@link MetadataValidationState} invalidations and updates
   */
  private readonly emitPartialCardinalities: boolean;

  protected baseMetadata: MetadataBindings = {
    state: new MetadataValidationState(),
    cardinality: { type: 'estimate', value: 0 },
    variables: [],
  };

  public constructor(
    store: RDF.Store | undefined,
    metadataAccumulator:
    (accumulatedMetadata: MetadataBindings, appendingMetadata: MetadataBindings) => Promise<MetadataBindings>,
    emitPartialCardinalities: boolean,
    dataFactory: ComunicaDataFactory,
    trackSources = false,
  ) {
    super(store);
    AggregatedStoreMemory.offsetEntityIds(this.getStore());
    this.metadataAccumulator = metadataAccumulator;
    this.emitPartialCardinalities = emitPartialCardinalities;
    this.dataFactory = dataFactory;
    this.trackSources = trackSources;
  }

  /**
   * Advances the entity id counter of an empty N3 store past V8's dense-elements threshold.
   * Reaches into N3 internals, so it checks what it finds and leaves the store alone otherwise.
   */
  protected static offsetEntityIds(store: RDF.Store): void {
    const entityIndex = (<{ _entityIndex?: { _id?: number } }> <unknown> store)._entityIndex;
    if (entityIndex && typeof entityIndex._id === 'number' && entityIndex._id < ENTITY_ID_OFFSET) {
      entityIndex._id = ENTITY_ID_OFFSET;
    }
  }

  public async importSource(url: string, source: IQuerySource, context: IActionContext): Promise<void> {
    if (!this.ended) {
      const AF = new AlgebraFactory();
      this.containedSources.add(url);
      const eventEmitter = this.import(source.queryQuads(AF.createPattern(
        this.dataFactory.variable('s'),
        this.dataFactory.variable('p'),
        this.dataFactory.variable('o'),
        this.dataFactory.variable('g'),
      ), context), url);
      await new Promise((resolve, reject) => {
        eventEmitter.on('end', resolve);
        eventEmitter.on('error', reject);
      });
    }
  }

  public override import(stream: RDF.Stream, source?: string): EventEmitter {
    if (this.ended) {
      return stream;
    }
    if (source === undefined || !this.trackSources) {
      super.import(stream);
      return stream;
    }
    // The recorded stream is what the store consumes and what is handed back, so a caller waiting
    // on `end` waits for the sources to be recorded as well as for the quads to be inserted
    const recorded = wrap<RDF.Quad>(stream).map((quad) => {
      this.recordSource(quad, source);
      return quad;
    });
    super.import(recorded);
    return recorded;
  }

  /**
   * Records `source` as a document that asserted the given quad.
   */
  protected recordSource(quad: RDF.Quad, source: string): void {
    let byPredicate = this.sourceIndex.get(quad.subject.value);
    if (byPredicate === undefined) {
      byPredicate = new Map();
      this.sourceIndex.set(quad.subject.value, byPredicate);
    }
    let byObject = byPredicate.get(quad.predicate.value);
    if (byObject === undefined) {
      byObject = new Map();
      byPredicate.set(quad.predicate.value, byObject);
    }

    const key = AggregatedStoreMemory.objectKey(quad.object);
    const known = byObject.get(key);
    if (known === undefined) {
      byObject.set(key, source);
      return;
    }
    if (typeof known === 'string') {
      const kept = AggregatedStoreMemory.moreSpecific(known, source);
      byObject.set(key, kept ?? [ known, source ]);
      return;
    }
    for (const [ index, existing ] of known.entries()) {
      const kept = AggregatedStoreMemory.moreSpecific(existing, source);
      if (kept !== undefined) {
        known[index] = kept;
        return;
      }
    }
    known.push(source);
  }

  /**
   * The documents that asserted the given quad, if any were recorded.
   */
  public getSources(quad: RDF.BaseQuad): string | string[] | undefined {
    return this.sourceIndex
      .get(quad.subject.value)
      ?.get(quad.predicate.value)
      ?.get(AggregatedStoreMemory.objectKey(quad.object));
  }

  /**
   * The narrower of two document URLs when one names a subtree the other lies in, and undefined
   * when neither contains the other and they are therefore two separate documents.
   */
  protected static moreSpecific(left: string, right: string): string | undefined {
    if (left === right) {
      return left;
    }
    if (AggregatedStoreMemory.contains(left, right)) {
      return right;
    }
    if (AggregatedStoreMemory.contains(right, left)) {
      return left;
    }
    return undefined;
  }

  /**
   * Whether `inner` lies under `outer`. The boundary character is what stops a document from
   * being taken for one whose name merely extends it, such as `/pods/15` for `/pods/150`.
   */
  protected static contains(outer: string, inner: string): boolean {
    if (!inner.startsWith(outer)) {
      return false;
    }
    const last = outer.codePointAt(outer.length - 1);
    if (last === 47 || last === 35) {
      return true;
    }
    const boundary = inner.codePointAt(outer.length);
    return boundary === 47 || boundary === 35 || boundary === 63;
  }

  /**
   * The key an object is held under in the source index.
   */
  protected static objectKey(object: RDF.Term): string {
    if (object.termType !== 'Literal') {
      return object.value;
    }
    return `"${object.value}${OBJECT_KEY_SEPARATOR}${object.datatype.value}${
      OBJECT_KEY_SEPARATOR}${object.language}`;
  }

  public hasRunningIterators(): boolean {
    return this.runningIterators.size > 0;
  }

  public override match(
    subject?: RDF.Term | null,
    predicate?: RDF.Term | null,
    object?: RDF.Term | null,
    graph?: RDF.Term | null,
  ): AsyncIterator<RDF.Quad> {
    // Wrap the raw stream in an AsyncIterator
    const rawStream = super.match(subject, predicate, object, graph);

    // The documents a quad came from travel on the quad itself, so that the step turning quads
    // into bindings can put them in the binding context without reaching back here
    const sourcedStream = this.trackSources ?
      wrap<RDF.Quad>(<any> rawStream).map((quad) => {
        const sources = this.getSources(quad);
        if (sources !== undefined) {
          (<IQuadWithSources> quad).sources = sources;
        }
        return quad;
      }) :
      rawStream;

    const iterator = new ClosableTransformIterator<RDF.Quad, RDF.Quad>(
      <any> sourcedStream,
      {
        autoStart: false,
        onClose: () => {
          // Running iterators are deleted once closed or destroyed
          this.runningIterators.delete(iterator);

          // Invoke listeners when all iterators have been closed
          if (!this.hasRunningIterators()) {
            for (const listener of this.allIteratorsClosedListeners) {
              listener();
            }
          }
        },
      },
    );

    // Expose the metadata property containing the cardinality
    let count = this.getStore().countQuads(subject!, predicate!, object!, graph!);
    const metadata: MetadataQuads = {
      state: new MetadataValidationState(),
      cardinality: {
        type: 'estimate',
        value: count,
      },
    };
    iterator.setProperty('metadata', metadata);
    iterator.setProperty('lastCount', count);

    if (this.emitPartialCardinalities) {
      // Every time a new quad is pushed into the iterator, update the metadata
      rawStream.on('quad', () => {
        iterator.setProperty('lastCount', ++count);
        this.updateMetadataState(iterator, count);
      });
    }

    // Store all running iterators until they close or are destroyed
    this.runningIterators.add(iterator);

    // Invoke creation listeners
    for (const listener of this.iteratorCreatedListeners) {
      listener();
    }

    return iterator;
  }

  public setBaseMetadata(metadata: MetadataBindings, updateStates: boolean): void {
    this.baseMetadata = { ...metadata };
    this.baseMetadata.cardinality = { type: 'exact', value: 0 };

    if (updateStates) {
      for (const iterator of this.runningIterators) {
        const count: number = iterator.getProperty('lastCount')!;
        this.updateMetadataState(iterator, count);
      }
    }
  }

  protected updateMetadataState(iterator: AsyncIterator<RDF.Quad>, count: number): void {
    // Append the given cardinality to the base metadata
    const metadataNew: MetadataBindings = {
      state: new MetadataValidationState(),
      cardinality: {
        type: 'estimate',
        value: count,
      },
      variables: [],
    };

    this.metadataAccumulator(this.baseMetadata, metadataNew)
      .then((accumulatedMetadata) => {
        accumulatedMetadata.state = new MetadataValidationState();

        // Set the new metadata, and invalidate the previous state
        const metadataToInvalidate = iterator.getProperty<MetadataQuads>('metadata');
        iterator.setProperty('metadata', accumulatedMetadata);
        metadataToInvalidate?.state.invalidate();
      })
      .catch(() => {
        // Void errors
      });
  }

  public addIteratorCreatedListener(listener: () => void): void {
    this.iteratorCreatedListeners.add(listener);
  }

  public removeIteratorCreatedListener(listener: () => void): void {
    this.iteratorCreatedListeners.delete(listener);
  }

  public addAllIteratorsClosedListener(listener: () => void): void {
    this.allIteratorsClosedListeners.add(listener);
  }

  public removeAllIteratorsClosedListener(listener: () => void): void {
    this.allIteratorsClosedListeners.delete(listener);
  }

  public clone(): IAggregatedStore {
    return new AggregatedStoreMemory(
      this.store,
      this.metadataAccumulator,
      this.emitPartialCardinalities,
      this.dataFactory,
      this.trackSources,
    );
  }
}
