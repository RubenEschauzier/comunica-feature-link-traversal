# Comunica Set Cache Indexed Offline Traversal Optimize Query Operation Actor

[![npm version](https://badge.fury.io/js/%40comunica%2Factor-optimize-query-operation-set-cache-indexed-offline-traversal.svg)](https://www.npmjs.com/package/@comunica/actor-optimize-query-operation-set-cache-indexed-offline-traversal)

A comunica Set Cache Indexed Offline Traversal Optimize Query Operation Actor.

This module is part of the [Comunica framework](https://github.com/comunica/comunica),
and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

[Click here if you just want to query with Comunica](https://comunica.dev/docs/query/).

## Install

```bash
$ yarn add @comunica/actor-optimize-query-operation-set-cache-indexed-offline-traversal
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-optimize-query-operation-set-cache-indexed-offline-traversal/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:optimize-query-operation/actors#set-cache-indexed-offline-traversal",
      "@type": "ActorOptimizeQueryOperationSetCacheIndexedOfflineTraversal"
    }
  ]
}
```

### Config Parameters

TODO: fill in parameters (this section can be removed if there are none)

* `someParam`: Description of the param
