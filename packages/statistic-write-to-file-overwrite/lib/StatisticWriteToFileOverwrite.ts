import type { ActionContextKey } from '@comunica/core';
import { StatisticBase } from '@comunica/statistic-base';
import type { IStatisticBase } from '@comunica/types';
import * as fsPromises from "fs/promises";
import * as fs from "fs";
import * as path from 'path';


/**
 * Class used to write result information to file for r3-metric tracking. It depends on a file which maps
 * base64 representations of queries that will be issued in the benchmark to file locations where the file 
 * should be written to. This writer overwrites the previous file with each received event. This is useful
 * for events where the entire history of the tracked statistic is emitted, like the topology tracker.
 */
export class StatisticWriteToFileOverwrite<T> extends StatisticBase<T> {
  public key: ActionContextKey<IStatisticBase<T>>;
  public writeQueue: Record<string, Promise<void>> = {};
  public statisticToWrite: IStatisticBase<T>; 

  public constructor(args: IStatisticWriteToFileOverwriteArgs) {
    super();
    this.statisticToWrite = args.statisticToWrite;
    let outputLocation: string;
    if (args.fileLocationBase64ToDir){
      const base64ToDir=  this.readBase64ToDir(new URL(args.fileLocationBase64ToDir).pathname);
      outputLocation = this.getFileLocation(
        args.query, new URL(args.baseDirectoryExperiment).pathname, base64ToDir
      );
    }
    else{
      outputLocation = path.join(args.baseDirectoryExperiment, 
        `${this.statisticToWrite.constructor.name}.txt`);
    }
    this.statisticToWrite.on((data: T) => {
      this.updateStatistic(outputLocation, data)
    });
  }

  public async writeToFileSafely(filePath: string, content: T): Promise<void> {
    const tempPath = `${filePath}.tmp`;

    // Add this write operation to the queue for this file
    if (!this.writeQueue[filePath]) {
      this.writeQueue[filePath] = Promise.resolve();
    }

    this.writeQueue[filePath] = this.writeQueue[filePath].then(async () => {
        try {
            // Write to a temporary file
            await fsPromises.writeFile(tempPath, JSON.stringify(content), { encoding: "utf8" });

            // Atomically replace the original file with the temporary file
            await fsPromises.rename(tempPath, filePath);
        } catch (error) {
            console.error("Error writing to file:", error);

            // Clean up temporary file only if it exists
            try {
                await fsPromises.access(tempPath);
                await fsPromises.unlink(tempPath);
            } catch (cleanupError: any) {
                if (cleanupError.code !== "ENOENT") {
                    console.error("Error cleaning up temporary file:", cleanupError);
                }
            }
        }
    });

    // Ensure errors in the queue don't break the chain
    this.writeQueue[filePath].catch(err => {
        console.error("Error in file queue:", err);
    });
  }

  public async updateStatistic(fileLocation: string, data: T): Promise<boolean> {
    this.writeToFileSafely(fileLocation, data);
    return true;
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


export interface IStatisticWriteToFileOverwriteArgs{
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
