import type { ActionContextKey } from '@comunica/core';
import { StatisticBase } from '@comunica/statistic-base';
import type { IStatisticBase, PartialResult } from '@comunica/types';
import { BunyanStreamProviderFile, ILoggerBunyanArgs, LoggerBunyan } from '@comunica/logger-bunyan';
import { Bindings } from '@comunica/utils-bindings-factory';
import * as fs from "fs";
import * as path from 'path';


/**
 * Class used to write result information to file for r3-metric tracking. It depends on a file which maps
 * base64 representations of queries that will be issued in the benchmark to file locations where the file 
 * should be written to. 
 */
export class StatisticWriteToFile extends StatisticBase<PartialResult> {
  public logger: LoggerBunyan
  public key: ActionContextKey<IStatisticBase<PartialResult>>;
  public statisticToWrite: IStatisticBase<PartialResult>;

  public constructor(fileLocationBase64ToDir: string, baseDirectoryExperiment: string,
    statisticsToWrite: IStatisticBase<PartialResult>, query: string
  ) {
    super();
    this.statisticToWrite = statisticsToWrite;
    const base64ToDir=  this.readBase64ToDir(fileLocationBase64ToDir);
    const outputLocation = this.getFileLocation(
      query, baseDirectoryExperiment, base64ToDir
    );

    const loggerOptions: ILoggerBunyanArgs = {
      name: 'comunica',
      streamProviders: [
        new BunyanStreamProviderFile({ level: 'info', path: outputLocation})
      ]
    };
    this.logger = new LoggerBunyan(loggerOptions)
    this.statisticToWrite.on((data: PartialResult) => {
      this.updateStatistic(data)
    });
  }

  public async updateStatistic(data: PartialResult): Promise<boolean> {
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

  public readBase64ToDir(location: string): Record<string, string>{
    return JSON.parse(fs.readFileSync(location, 'utf-8'));
  }

  public getFileLocation(
    query: string, 
    baseDirectoryExperiment: string, 
    base64ToDir: Record<string, string>
  ){
    // Convert query to base64 string
    const base64Query = btoa(query.trim());
    // Use that to find the directory it should go to
    const directory = base64ToDir[base64Query];
    if (!directory){
      throw new Error(`No matching query found for ${query}`);
    }
    const fullPathDirectory = path.join(baseDirectoryExperiment, directory);
    // Count the number of files already written to directory to prevent overwriting existing runs
    const nFilesInPath = fs.readdirSync(fullPathDirectory).length;
    const fullPath = path.join(fullPathDirectory, 
      `${this.statisticToWrite.constructor.name}_${nFilesInPath}.txt`);
    return fullPath
  }

  public extractFileNumber(fileName: string) {
    const match = fileName.match(/_(\d+)\.txt$/); // Matches "_<digits>.txt" at the end
    return match ? parseInt(match[1], 10) : 0; // Return the number as an integer or null if no match
  }

}
