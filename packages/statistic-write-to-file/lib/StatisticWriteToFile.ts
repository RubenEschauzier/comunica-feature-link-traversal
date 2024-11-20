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

  public constructor(fileLocation: string, statisticsToWrite: IStatisticBase<PartialResult>, queryNum: number = 0) {
    super();
    const loggerOptions: ILoggerBunyanArgs = {
      name: 'comunica',
      streamProviders: [
        new BunyanStreamProviderFile({ level: 'info', path: this.insertQueryNumber(fileLocation, queryNum) })
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
  public insertQueryNumber(fileLocation: string, queryNum: number){
    const lastDotIndex = fileLocation.lastIndexOf('.');
    if (lastDotIndex === -1) {
        // No extension found, append the queryNum at the end
        return `${fileLocation}_${queryNum}`;
    }
    
    const name = fileLocation.substring(0, lastDotIndex);
    const extension = fileLocation.substring(lastDotIndex);

    return `${name}_${queryNum}${extension}`;
  }
}
