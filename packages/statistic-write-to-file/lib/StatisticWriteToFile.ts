import type { ActionContextKey } from '@comunica/core';
import { StatisticBase } from '@comunica/statistic-base';
import type { IStatisticBase, PartialResult } from '@comunica/types';
import { BunyanStreamProviderFile, ILoggerBunyanArgs, LoggerBunyan } from '@comunica/logger-bunyan';
import { Bindings } from '@comunica/utils-bindings-factory';


// This is made specifically for the intermediate results, more general framework might be needed
export class StatisticWriteToFile extends StatisticBase<PartialResult> {
  public fileLocation: string;
  public logger: LoggerBunyan
  public key: ActionContextKey<IStatisticBase<PartialResult>>;

  public constructor(fileLocation: string, statisticsToWrite: IStatisticBase<PartialResult>) {
    super();
    const loggerOptions: ILoggerBunyanArgs = {
      name: 'comunica',
      streamProviders: [
        new BunyanStreamProviderFile({ level: 'info', path: fileLocation })
      ]
    };
    this.logger = new LoggerBunyan(loggerOptions)
    statisticsToWrite.on((data: PartialResult) => {
      this.updateStatistic(data)
    });
  }

  public async updateStatistic(data: PartialResult): Promise<boolean> {
    // await this.writeToFileSafely(fileLocation, data);
    if (data.type === 'bindings'){
      const binding = <Bindings> data.data;
      const reducedData = {
        data: binding.toString(),
        operation: data.metadata['operation']
      }
      this.logger.info('update', reducedData)
      return true;  
    }
    return false;
  }
}
