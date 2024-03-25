import { KeysRdfResolveHypermediaLinks } from '@comunica/context-entries-link-traversal';
import type { ICliArgsHandler } from '@comunica/types';
import type { Argv } from 'yargs';

export class CliArgsHandlerReturnTopology implements ICliArgsHandler {
  public populateYargs(argumentsBuilder: Argv<any>): Argv<any> {
    return argumentsBuilder
      .options({
        returnTopology: {
          type: 'boolean',
          describe: 'If endpoint should return the tracked topology in result',
        },
      });
  }

  public async handleArgs(args: Record<string, any>, context: Record<string, any>): Promise<void> {
    if (args.annotateSources) {
      context["returnTopology"] = args.returnTopology;
    }
  }
}
