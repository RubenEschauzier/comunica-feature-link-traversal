import type { ActionContextKey } from '@comunica/core';
import { StatisticBase } from '@comunica/statistic-base';
import type { IStatisticBase } from '@comunica/types';
import * as fs from "fs/promises";

// If file size is an issue:
// https://stackoverflow.com/questions/75501488/design-pattern-for-writing-to-a-file-from-multiple-async-functions
// Write to new temp file, once atomicly rename new file to old file with state
// Make logger, allow as argument array of statistics and for each statistic attach listener that simply logs JSON object with
// Statistic name and the data.
export class StatisticWriteToFileOverwrite<T> extends StatisticBase<T> {
  public key: ActionContextKey<IStatisticBase<T>>;
  public writeQueue: Record<string, Promise<void>> = {};

  public constructor(fileLocation: string, statisticsToWrite: IStatisticBase<T>, queryNum: number = 0) {
    super();
    const fileWithNum = this.insertQueryNumber(fileLocation, queryNum);
    statisticsToWrite.on((data: T) => {
      this.updateStatistic(fileWithNum, data)
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
            await fs.writeFile(tempPath, JSON.stringify(content), { encoding: "utf8" });

            // Atomically replace the original file with the temporary file
            await fs.rename(tempPath, filePath);
        } catch (error) {
            console.error("Error writing to file:", error);

            // Clean up temporary file only if it exists
            try {
                await fs.access(tempPath);
                await fs.unlink(tempPath);
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
