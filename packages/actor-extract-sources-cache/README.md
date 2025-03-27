# Comunica Cache Extract Sources Actor

[![npm version](https://badge.fury.io/js/%40comunica%2Factor-extract-sources-cache.svg)](https://www.npmjs.com/package/@comunica/actor-extract-sources-cache)

A comunica Cache Extract Sources Actor.

This module is part of the [Comunica framework](https://github.com/comunica/comunica),
and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

[Click here if you just want to query with Comunica](https://comunica.dev/docs/query/).

## Install

```bash
$ yarn add @comunica/actor-extract-sources-cache
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-extract-sources-cache/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:extract-sources/actors#cache",
      "@type": "ActorExtractSourcesCache"
    }
  ]
}
```

### Config Parameters

TODO: fill in parameters (this section can be removed if there are none)

* `someParam`: Description of the param
