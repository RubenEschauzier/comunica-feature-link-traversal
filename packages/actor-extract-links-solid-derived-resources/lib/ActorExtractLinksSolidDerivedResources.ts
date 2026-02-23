import { ActorInitQueryBase, QueryEngineBase } from '@comunica/actor-init-query';
import { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import { ActorExtractLinks, IActionExtractLinks, IActorExtractLinksOutput, IActorExtractLinksArgs } from '@comunica/bus-extract-links';
import { IActorDereferenceOutput, MediatorDereference } from "@comunica/bus-dereference";
import { KeysInitQuery, KeysQueryOperation, KeysQuerySourceIdentify, KeysStatistics } from '@comunica/context-entries';
import { KeysRdfJoin } from '@comunica/context-entries-link-traversal';
import { TestResult, IActorTest, passTestVoid, failTest, IActorArgs, ActionContext } from '@comunica/core';
import { IActionContext, ILink } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { storeStream } from 'rdf-store-stream';
import { FragmentSelectorShape } from '@comunica/types';

/**
 * A comunica Solid Derived Resources Extract Links Actor.
 */
export class ActorExtractLinksSolidDerivedResources extends ActorExtractLinks {
  protected readonly derivedResourcePredicates: string[];
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  public readonly mediatorDereference: MediatorDereference;
  public readonly queryEngine: QueryEngineBase;

  public constructor(args: IActorExtractLinksSolidDerivedResourcesArgs) {
    super(args);
    this.derivedResourcePredicates = args.derivedResourcePredicates;
    this.mediatorDereferenceRdf = args.mediatorDereferenceRdf;
    this.mediatorDereference = args.mediatorDereference
    this.queryEngine = new QueryEngineBase(args.actorInitQuery);
  }

  public async test(action: IActionExtractLinks): Promise<TestResult<IActorTest>> {
    if (!action.context.get(KeysInitQuery.query)) {
      return failTest(`Actor ${this.name} can only work in the context of a query.`);
    }
    return passTestVoid();
  }

  public async run(action: IActionExtractLinks): Promise<IActorExtractLinksOutput> {
    // Determine links to type indexes
    const derivedResources = [ ...await this.extractDerivedResourceLinks(action.metadata) ];
    const derivedResourcesRaw: IDerivedResourceRaw[][] = (await Promise.all(derivedResources
      .map(derivedResource => this.dereferenceDerivedResources(derivedResource, action.context))));
    const derivedResourcesUnidentified: IDerivedResourceUnidentified[] = await Promise.all(
      derivedResourcesRaw.flat().map(resource => this.dereferenceFilter(resource)
    ));
    // console.log(action.context.get(KeysInitQuery.querySourcesUnidentified))
    console.log(action.context.get(KeysQueryOperation.querySources))
    // TODO: After extracting any derived resources set handled to true for the URLs I've dereferenced
    // const traversalManager = action.context.get(Keys)
    return { links: [] };
  }

  /**
   * Extract links to type index from the metadata stream.
   * @param metadata A metadata quad stream.
   */
  public extractDerivedResourceLinks(metadata: RDF.Stream): Promise<Set<string>> {
    metadata.setMaxListeners(20);
    return new Promise<Set<string>>((resolve, reject) => {
      const derivedResourcesInner: Set<string> = new Set();

      // Forward errors
      metadata.on('error', reject);

      // Invoke callback on each metadata quad
      metadata.on('data', (quad: RDF.Quad) => {
        if (this.derivedResourcePredicates.includes(quad.predicate.value)) {
          derivedResourcesInner.add(quad.object.value);
        }
      });

      // Resolve to discovered links
      metadata.on('end', () => {
        resolve(derivedResourcesInner);
      });

    });
  }
  
  public async dereferenceDerivedResources(derivedResource: string, context: IActionContext): Promise<IDerivedResourceRaw[]> {
    // Parse the type index document
    const response = await this.mediatorDereferenceRdf.mediate({ url: derivedResource, context });
    const store = await storeStream(response.data);

    // Query the document to extract all type registrations
    const bindingsArray = await (await this.queryEngine
      .queryBindings(`
        SELECT ?resource ?template ?selector ?filter WHERE {
          ?pod <urn:npm:solid:derived-resources:derivedResource> ?resource .
          ?resource <urn:npm:solid:derived-resources:template> ?template ;
            <urn:npm:solid:derived-resources:selector> ?selector ;
            <urn:npm:solid:derived-resources:filter> ?filter .
        }`, {
        sources: [ store ],
        [KeysQuerySourceIdentify.traverse.name]: false,
        [KeysRdfJoin.skipAdaptiveJoin.name]: true,
        [KeysStatistics.skipStatisticTracking.name]: true,
        lenient: true,
      })).toArray();
    
    // Collect derived resources, aggregate selectors belonging to same resource
    const derivedResourcesRaw: Record<string, IDerivedResourceRaw> = {};

    for (const bindings of bindingsArray) {
      const resourceIdentifier = bindings.get('resource')!.value;
      if (!derivedResourcesRaw[resourceIdentifier]){
        derivedResourcesRaw[resourceIdentifier] = {
          template: bindings.get('template')!.value,
          selectors: [],
          filterUri: { url: bindings.get('filter')!.value }
        }
      }
      derivedResourcesRaw[resourceIdentifier].selectors.push(bindings.get('selector')!.value);
    }
    return [...Object.values(derivedResourcesRaw)];
  }

  public async dereferenceFilter(derivedResourcesUnidentified: IDerivedResourceRaw):
   Promise<IDerivedResourceUnidentified>{
    const response: IActorDereferenceOutput = await this.mediatorDereference.mediate(
      { 
        url: derivedResourcesUnidentified.filterUri.url, 
        acceptErrors: true,
          method: "GET",
          // We use the headers to ensure the server knows we are looking for RDF
          headers: new Headers({
            "Accept": "text/turtle,application/n-quads,application/trig,application/ld+json,application/sparql-query"
          }),
          mediaTypes: async () => ({
            // SHACL-based filters use turtle
            "text/turtle": 1.0,          
            // Quad pattern indexes are represented as json
            "application/json": 0.4,  
            // SPARQL-based filters
            "application/sparql-query": 0.7, 
            // QPF uses plain text filter files
            "text/plain": .4,
            // Fallback (TODO: Should we even have this?)
            "*/*": 0.1                     
          }),
          context: new ActionContext(),
      }
    );
    const rawText = await this.streamToString(response.data);
    const cleanText = rawText.trim();

    return { ...derivedResourcesUnidentified, filter: cleanText }
  }

  private streamToString(stream: NodeJS.ReadableStream): Promise<string> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
      stream.on('error', (err) => reject(err));
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });
  }
  /**
   * node engines/query-sparql-link-traversal-solid/bin/query.js -q "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX snvoc: <http://solidbench-server:3000/www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>
SELECT ?messageId ?messageCreationDate ?messageContent WHERE {
  ?message snvoc:hasCreator <http://solidbench-server:3000/pods/00000000000000000933/profile/card#me>;
    rdf:type snvoc:Post;
    snvoc:content ?messageContent;
    snvoc:creationDate ?messageCreationDate;
    snvoc:id ?messageId.
}
" --idp void --lenient
   */

  // //TODO: Move this logic into actors
  // private async parseFilterFile(response: Response) {
  //   const contentType = response.headers.get('content-type') || '';
    
  //   const rawText = await response.text();
  //   const cleanText = rawText.trim();

  //   // 2. QPF Check: The file just contains "qpf" (Plain Text)
  //   if (cleanText === 'qpf') {
  //     return {
  //       type: 'QPF',
  //       config: cleanText
  //     };
  //   }

  //   // 3. Quad Pattern Index Check: Expecting a JSON object
  //   if (contentType.includes('application/json') || cleanText.startsWith('{')) {
  //     try {
  //       const jsonObj = JSON.parse(cleanText);
  //       return {
  //         type: 'INDEX',
  //         config: jsonObj // Contains the s, p, o, g and Variable fields
  //       };
  //     } catch (e) {
  //       console.warn("Looked like JSON, but failed to parse.", e);
  //     }
  //   }

  //   // 4. SPARQL Check: Expecting a CONSTRUCT query
  //   if (contentType.includes('application/sparql-query') || cleanText.toUpperCase().includes('CONSTRUCT')) {
  //     // Optional: Parse the SPARQL string into an AST using sparql.js so your engine can reason over it
  //     const sparqlParser = new SparqlParser();
  //     return {
  //       type: 'SPARQL',
  //       config: rawText,
  //       ast: sparqlParser.parse(rawText) 
  //     };
  //   }

  //   // 5. SHACL Check: Expecting text/turtle
  //   if (contentType.includes('text/turtle') || cleanText.includes('@prefix') || cleanText.includes('sh:')) {
  //     const n3Parser = new N3Parser({ format: 'text/turtle', baseIRI: response.url });
  //     const store = new Store();
      
  //     // Parse the turtle string into an N3 store synchronously
  //     store.addQuads(n3Parser.parse(rawText));
      
  //     return {
  //       type: 'SHACL',
  //       config: store // You can now query the shapes with store.getQuads(...)
  //     };
  //   }

  //   throw new Error(`Unrecognized filter file format. Content-Type: ${contentType}`);
  // }
}


export interface IActorExtractLinksSolidDerivedResourcesArgs
  extends IActorExtractLinksArgs {
  /**
   * The derived resource predicate URLs that will be followed.
   * @default {http://www.w3.org/2007/05/powder-s#describedby}
   */
  derivedResourcePredicates: string[];
  /**
   * An init query actor that is used to query shapes.
   * @default {<urn:comunica:default:init/actors#query>}
   */
  actorInitQuery: ActorInitQueryBase;
  /**
   * The Dereference RDF mediator
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * The dereference mediator for obtaining the filter files
   */
  mediatorDereference: MediatorDereference;
}

export interface IDerivedResourceRaw{
  /**
   * Derived resource template name
   */
  template: string;
  /**
   * The strings representing what data is in the derived resource. This can be glob patterns, specific directories,
   * or files. For directories we represent it as glob pattern. 
   */
  selectors: string[]
  /**
   * URI pointing to the file containing the filter, which produced the data.
   */
  filterUri: ILink
}

export interface IDerivedResourceUnidentified{
  /**
   * Derived resource template name
   */
  template: string;
  /**
   * The strings representing what data is in the derived resource. This can be glob patterns, specific directories,
   * or files. For directories we represent it as glob pattern. 
   */
  selectors: string[]
  /**
   * Filter string obtained from derived resource. This will be used in bus-derived-resource-identify to determine
   * the selector shape of the resource and the appropriate actor to use this derived resource.
   */
  filter: string
}

export interface IDerivedResource {
  /**
   * Derived resource template name
   */
  template: string;
  /**
   * The strings representing what data is in the derived resource. This can be glob patterns, specific directories,
   * or files. For directories we represent it as glob pattern. 
   */
  selectors: string[]
  /**
   * Filter string obtained from derived resource. This will be used in bus-derived-resource-identify to determine
   * the selector shape of the resource and the appropriate actor to use this derived resource.
   */
  filter: string
  /**
   * The selector shape of the derived resouce.
   */
  selectorShape: FragmentSelectorShape  
  /**
   * Performance coefficients, used to determine the best resource for a given task when multiple resources
   * can be used
   */
  resourceCoefficients: {
    /**
     * How cheap or expensive the resource is server-side
     * e.g. a parameterized query requires full query execution, while precomputed queries or QPF require
     * less compute
     */
    compute: number,
    /**
     * Number of requests required to obtain the full resource. 
     * e.g. a QPF requires more request to obtain the data, while a parameterized single triple pattern
     * query requires only one
     */
    requests: number,
    /**
     * The selectivity of the resource to answer a question
     * e.g. a parameterized join query is highly selective, while a union-based query must be filtered
     */
    selecitivty: number
  }
}
