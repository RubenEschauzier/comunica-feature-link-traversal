#!/usr/bin/env node
import { HttpServiceSparqlEndpoint } from '@comunica/actor-init-query-topology';
import { CliArgsHandlerSolidAuth } from '@comunica/query-sparql-solid';
import { CliArgsHandlerAnnotateSources } from '../lib/CliArgsHandlerAnnotateSources';
import { CliArgsHandlerReturnTopology } from '../lib/cliArgsHandlerReturnTopology';

const defaultConfigPath = `${__dirname}/../config/config-default.json`;

HttpServiceSparqlEndpoint.runArgsInProcess(
  process.argv.slice(2),
  process.stdout,
  process.stderr,
  `${__dirname}/../`,
  process.env,
  defaultConfigPath,
  code => {
    process.exit(code);
  },
  [
    new CliArgsHandlerSolidAuth(),
    new CliArgsHandlerAnnotateSources(),
    new CliArgsHandlerReturnTopology()
  ],
).catch(error => process.stderr.write(`${error.message}/n`));
