# Comunica Extract Links Query Source Dereference Link Actor

A [Query Source Dereference Link](https://github.com/comunica/comunica/tree/master/packages/bus-query-source-dereference-link) actor
that follows links in the answers of the known query sources, as link traversal does in documents.

A query source given with a type, such as a SPARQL endpoint, is a known member of the federation.
Link traversal takes it up as a seed with that type forced, and queries it alongside the documents it reads.
This actor wraps the source such a seed dereferences to.
For every binding the source answers, the triples it matched in the triple patterns sent to it are rebuilt,
and streamed into the [extract links bus](https://github.com/comunica/comunica-feature-link-traversal/tree/master/packages/bus-extract-links)
under the url of the source.
The links found are handed to the link traversal manager,
which keeps traversal running until they are handed over.

Only links with a forced source type are wrapped, which only seeds from the query sources have.
Resources found during traversal, such as derived resources, are identified by their own actors,
which extract their links already.

This module is part of the [Comunica framework](https://github.com/comunica/comunica),
and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

## Install

```bash
$ yarn add @comunica/actor-query-source-dereference-link-extract-links
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```json
{
  "@context": [
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-query-source-dereference-link-extract-links/^0.0.0/components/context.jsonld"
  ],
  "actors": [
    {
      "@id": "urn:comunica:default:query-source-dereference-link/actors#extract-links",
      "@type": "ActorQuerySourceDereferenceLinkExtractLinks",
      "mediatorQuerySourceDereferenceLink": { "@id": "urn:comunica:default:query-source-dereference-link/mediators#main" },
      "mediatorExtractLinks": { "@id": "urn:comunica:default:extract-links/mediators#main" },
      "beforeActors": [
        { "@id": "urn:comunica:default:query-source-dereference-link/actors#force-sparql" },
        { "@id": "urn:comunica:default:query-source-dereference-link/actors#dereference" }
      ]
    }
  ]
}
```

### Config Parameters

* `mediatorQuerySourceDereferenceLink`: A mediator over the [Query Source Dereference Link bus](https://github.com/comunica/comunica/tree/master/packages/bus-query-source-dereference-link).
* `mediatorExtractLinks`: A mediator over the [extract links bus](https://github.com/comunica/comunica-feature-link-traversal/tree/master/packages/bus-extract-links).
