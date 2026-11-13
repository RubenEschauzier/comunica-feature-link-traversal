# Comunica Hypermedia Links Queue Wrapper for Known Sources

An [RDF Resolve Hypermedia Links Queue](https://github.com/comunica/comunica/tree/master/packages/bus-rdf-resolve-hypermedia-links-queue) actor
that wraps over another link queue provided by the bus,
and keeps traversal out of the namespaces of the known query sources.

A query source given with a type, such as a SPARQL endpoint, is a known member of the federation,
and the only way its data is accessed.
Links into the namespaces it covers are not traversed, seeds included,
as declared in the context entry `@comunica/bus-rdf-resolve-hypermedia-links:sourceNamespaces`:
a map from a source's url to the url prefixes of the documents whose data it serves.
The source itself is still taken up:
link traversal takes it up as a seed with its type forced, and its url lies outside those namespaces.
Declared namespaces of sources that are not given with the query are ignored.

For example:
```json
{
  "sources": [
    "http://example.org/shop/product1",
    { "type": "sparql", "value": "http://sparql.example.org/shop" }
  ],
  "@comunica/bus-rdf-resolve-hypermedia-links:sourceNamespaces": {
    "http://example.org/shop/sparql": ["http://example.org/shop/"]
  }
}
```

This module is part of the [Comunica framework](https://github.com/comunica/comunica),
and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

## Install

```bash
$ yarn add @comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-known-sources
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```json
{
  "@context": [
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-rdf-resolve-hypermedia-links-queue-wrapper-known-sources/^0.0.0/components/context.jsonld"
  ],
  "actors": [
    {
      "@id": "urn:comunica:default:rdf-resolve-hypermedia-links-queue/actors#wrapper-known-sources",
      "@type": "ActorRdfResolveHypermediaLinksQueueWrapperKnownSources",
      "beforeActors": { "@id": "urn:comunica:default:rdf-resolve-hypermedia-links-queue/actors#fifo" },
      "mediatorRdfResolveHypermediaLinksQueue": { "@id": "urn:comunica:default:rdf-resolve-hypermedia-links-queue/mediators#main" }
    }
  ]
}
```

### Config Parameters

* `mediatorRdfResolveHypermediaLinksQueue`: A mediator over the [RDF Resolve Hypermedia Links Queue bus](https://github.com/comunica/comunica/tree/master/packages/bus-rdf-resolve-hypermedia-links-queue).
