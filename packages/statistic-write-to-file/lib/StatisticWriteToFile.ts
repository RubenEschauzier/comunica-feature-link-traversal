import type { ActionContextKey } from '@comunica/core';
import { StatisticBase } from '@comunica/statistic-base';
import type { IStatisticBase, PartialResult } from '@comunica/types';
import { BunyanStreamProviderFile, ILoggerBunyanArgs, LoggerBunyan } from '@comunica/logger-bunyan';
import { Bindings } from '@comunica/utils-bindings-factory';
import * as fs from "fs";
import * as path from 'path';
import { KeysMergeBindingsContext } from '@comunica/context-entries';
import type * as RDF from '@rdfjs/types';

/**
 * Class used to write result information to file for r3-metric tracking. It depends on a file which maps
 * base64 representations of queries that will be issued in the benchmark to file locations where the file 
 * should be written to. 
 */
export class StatisticWriteToFile extends StatisticBase<PartialResult> {
  public logger: LoggerBunyan
  public key: ActionContextKey<IStatisticBase<PartialResult>>;
  public statisticToWrite: IStatisticBase<PartialResult>;

  public constructor(args: IStatisticWriteToFileArgs) {
    super();
    this.statisticToWrite = args.statisticToWrite;
    let outputLocation: string;
    if (args.fileLocationBase64ToDir){
      const base64ToDir=  this.readBase64ToDir(new URL(args.fileLocationBase64ToDir).pathname);
      outputLocation = this.getFileLocation(
        args.query, new URL(args.baseDirectoryExperiment).pathname, base64ToDir
      );
      outputLocation = `file:///${outputLocation}`
    }
    else{
      outputLocation = path.join(args.baseDirectoryExperiment, 
        `${this.statisticToWrite.constructor.name}.txt`);
    }
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
      this.consumeAttributionStream(binding, data);
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

  public consumeAttributionStream(binding: Bindings, data: PartialResult){
    // TODO: This method will consume the stream, possibly BEFORE a prioritization algorithm has access to it.
    // we should be cloning streams or smthing.
    const sources = binding.getContextEntry(KeysMergeBindingsContext.sourcesBindingStream);
    if (!sources){
      console.error("No sources attached to binding writing result to file.");
      throw new Error("No sources attached to binding writing result to file.");
    }
    const sourceQuadsProcessed = new Set();
    // Sources are streams of provenance quads (including possible duplicates)
    sources.on('data', (data: RDF.BaseQuad) => {
      // Provenance is on object of triple
      const prov = data.object.value;
      // Filter duplicates
      if (!sourceQuadsProcessed.has(prov)) {
        sourceQuadsProcessed.add(prov);
      }
    });
    sources.on('end', () => {
      const reducedData = {
        data: binding.toString(),
        operation: data.metadata['operation'],
        provenance: JSON.stringify(Array.from(sourceQuadsProcessed))
      }
      this.logger.info('update', reducedData)
    });
  }
}

export interface IStatisticWriteToFileArgs{
  query: string,
  statisticToWrite: IStatisticBase<any>,
  /**
   * Base directory the experiment should be saved to.
   */
  baseDirectoryExperiment: string,

  /**
   * Filelocation for dictionary mapping base64 representations of query 
   * the directory this query run should be saved to. If undefined the
   * writer will write to baseDirectoryExperiment directory.
   */
  fileLocationBase64ToDir?: string,
}
