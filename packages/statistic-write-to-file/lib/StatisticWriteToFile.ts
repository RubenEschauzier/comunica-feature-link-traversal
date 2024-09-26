import type { ActionContextKey } from '@comunica/core';
import { StatisticBase } from '@comunica/statistic-base';
import type { IStatisticBase } from '@comunica/types';
import { BunyanStreamProviderFile, ILoggerBunyanArgs, LoggerBunyan } from '@comunica/logger-bunyan';

// If file size is an issue:
// https://stackoverflow.com/questions/75501488/design-pattern-for-writing-to-a-file-from-multiple-async-functions
// Write to new temp file, once atomicly rename new file to old file with state
// Make logger, allow as argument array of statistics and for each statistic attach listener that simply logs JSON object with
// Statistic name and the data.
export class StatisticWriteToFile<T> extends StatisticBase<T> {
  public fileLocation: string;
  public logger: LoggerBunyan
  public key: ActionContextKey<IStatisticBase<T>>;

  public constructor(fileLocation: string, statisticsToWrite: IStatisticBase<T>) {
    super();
    statisticsToWrite.on((data: T) => {
      this.updateStatistic(data)
    });
    const loggerOptions: ILoggerBunyanArgs = {
      name: 'comunica',
      streamProviders: [
        new BunyanStreamProviderFile({ level: 'info', path: fileLocation })
      ]
    };
    this.logger = new LoggerBunyan(loggerOptions)
  }

  public updateStatistic(data: T): boolean {
    this.logger.info('update', data)
    return true;
  }
}
