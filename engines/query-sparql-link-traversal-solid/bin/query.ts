#!/usr/bin/env node
import { KeysInitQuery } from '@comunica/context-entries';
import { ActionContext } from '@comunica/core';
import { CliArgsHandlerSolidAuth } from '@comunica/query-sparql-solid';
import { runArgsInProcessStatic } from '@comunica/runner-cli';
import { CliArgsHandlerAnnotateSources } from '../lib/CliArgsHandlerAnnotateSources';

const cliArgsHandlerSolidAuth = new CliArgsHandlerSolidAuth();

// Node exits once nothing is left to wait on, even while a result stream has not ended. A stream
// that stalls would then end the process without an error, cutting off the results
let done = false;
process.on('beforeExit', () => {
  if (!done) {
    process.stderr.write('Query evaluation stalled: nothing is left to wait on, but the results did not end\n');
    process.exit(1);
  }
});
// eslint-disable-next-line import/extensions,ts/no-require-imports,ts/no-var-requires
runArgsInProcessStatic(require('../engine-default.js')(), {
  context: new ActionContext({
    [KeysInitQuery.cliArgsHandlers.name]: [
      cliArgsHandlerSolidAuth,
      new CliArgsHandlerAnnotateSources(),
    ],
  }),
  onDone() {
    done = true;
    if (cliArgsHandlerSolidAuth.session) {
      cliArgsHandlerSolidAuth.session.logout()
        // eslint-disable-next-line no-console
        .catch(error => console.log(error));
    }
  },
});
