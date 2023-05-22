#!/usr/bin/env node

// Test Command:
// node bin/query.js https://fragments.dbpedia.org/2015/en "SELECT DISTINCT * WHERE { ?s ?p <http://dbpedia.org/resource/Belgium>. ?s ?p ?o} LIMIT 10"
import { HttpServiceSparqlEndpoint } from '@comunica/actor-init-query';

const defaultConfigPath = `${__dirname}/../config/config-default.json`;

HttpServiceSparqlEndpoint.runArgsInProcess(process.argv.slice(2), process.stdout, process.stderr, `${__dirname}/../`, process.env, defaultConfigPath, code => process.exit(code))
  .catch(error => process.stderr.write(`${error.message}/n`));
