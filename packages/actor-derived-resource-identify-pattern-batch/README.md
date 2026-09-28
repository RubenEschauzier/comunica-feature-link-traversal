# Comunica Pattern Batch Derived Resource Identify Actor

[![npm version](https://badge.fury.io/js/%40comunica%2Factor-derived-resource-identify-pattern-batch.svg)](https://www.npmjs.com/package/@comunica/actor-derived-resource-identify-pattern-batch)

A comunica Pattern Batch Derived Resource Identify Actor.

This module is part of the [Comunica framework](https://github.com/comunica/comunica),
and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

[Click here if you just want to query with Comunica](https://comunica.dev/docs/query/).

## Install

```bash
$ yarn add @comunica/actor-derived-resource-identify-pattern-batch
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-derived-resource-identify-pattern-batch/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:derived-resource-identify/actors#pattern-batch",
      "@type": "ActorDerivedResourceIdentifyPatternBatch"
    }
  ]
}
```

### Config Parameters

TODO: fill in parameters (this section can be removed if there are none)

* `someParam`: Description of the param
