# Comunica Walk Up Derived Resource Discover Actor

[![npm version](https://badge.fury.io/js/%40comunica%2Factor-derived-resource-discover-walk-up.svg)](https://www.npmjs.com/package/@comunica/actor-derived-resource-discover-walk-up)

A comunica Walk Up Derived Resource Discover Actor.

From every traversed document, it reads the descriptions (`.meta`) of the containers that document lies in,
and discovers the resources they link to, such as the SPARQL endpoint of a pod. A pod rarely links its own
storage root from its documents, so without this actor the description of the root is never found.

## Assumptions

This actor guesses where pods are by their depth in the path, not by what the server says about them:

* **Pods lie at a fixed depth.** It reads the descriptions of the containers from the top of the path down
  to `maxDepth`, not those of the containers right above the document. The default of `1` fits servers that
  put pods right below the server root (`http://host/producer2/`, as in FedShop). Where they lie deeper, such
  as `http://host/pods/<id>/` in SolidBench, `maxDepth` must be raised to reach them, and every container
  above them is read too, including one such as `/pods/` that holds every pod on the server. Only the server
  root itself is never read.
* **Every container it reads is held back while its description is fetched.** A description that links to
  nothing, or cannot be read (such as a `404`), discovers nothing and releases the container, at the cost of
  one request and its delay. A description that does not answer keeps the container's links from being
  crawled until its request fails.
* **Each container costs a request**, once per query: one per pod at depth `1`, and one per container at
  every level deeper.

A sturdier actor would walk up to the container the server types as the storage root (`pim:Storage`), or
follow the storage description the server advertises, rather than rely on depth.

This module is part of the [Comunica framework](https://github.com/comunica/comunica),
and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

[Click here if you just want to query with Comunica](https://comunica.dev/docs/query/).

## Install

```bash
$ yarn add @comunica/actor-derived-resource-discover-walk-up
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-derived-resource-discover-walk-up/^0.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:derived-resource-discover/actors#walk-up",
      "@type": "ActorDerivedResourceDiscoverWalkUp",
      "maxDepth": 1,
      "mediatorDereferenceRdf": { "@id": "urn:comunica:default:dereference-rdf/mediators#main" },
      "mediatorDerivedResourceDiscover": { "@id": "urn:comunica:default:derived-resource-discover/mediators#main" }
    }
  ]
}
```

### Config Parameters

* `maxDepth`: How deep the containers lie whose descriptions are read, in path segments, _defaults to `1`_ (a container right below the server root).
* `mediatorDereferenceRdf`: The mediator fetching the descriptions.
* `mediatorDerivedResourceDiscover`: The mediator discovering the resources in a description, which is the one of this bus.
