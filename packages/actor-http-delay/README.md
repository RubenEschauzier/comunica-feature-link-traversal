# Comunica HTTP Delay Actor

An [HTTP](https://github.com/comunica/comunica/tree/master/packages/bus-http) actor that applies delays to requests,
to simulate network latency in scenarios where actual latency cannot be applied easily.
The actor applies values using a uniform distribution over the specified range.

This module is part of the [Comunica framework](https://github.com/comunica/comunica),
and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

[Click here if you just want to query with Comunica](https://comunica.dev/docs/query/).

## Install

```bash
$ yarn add @comunica/actor-http-delay
```

## Configure

After installing, this package can be added to your engine's configuration as follows:

```json
{
  "@context": [
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-http-delay/^4.0.0/components/context.jsonld"
  ],
  "actors": [
    {
      "@id": "urn:comunica:default:http/actors#delay",
      "@type": "ActorHttpDelay"
    }
  ]
}
```

### Config Parameters

* `mediatorHttp`: A mediator over the [HTTP bus](https://github.com/comunica/comunica/tree/master/packages/bus-http).
* `average`: The average delay in milliseconds to apply. Defaults to `40`.
* `delta`: The maximum delta from the average for the uniform distribution, applied in both directions. Defaults to `10`.
