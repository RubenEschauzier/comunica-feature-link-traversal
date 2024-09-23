import type { ActionContextKey } from '@comunica/core';
import { StatisticBase } from '@comunica/statistic-base';
import type { ILink, IQuerySource, IStatisticBase } from '@comunica/types';

// https://stackoverflow.com/questions/75501488/design-pattern-for-writing-to-a-file-from-multiple-async-functions
// Write to new temp file, once atomicly rename new file to old file with state
// Make logger, allow as argument array of statistics and for each statistic attach listener that simply logs JSON object with
// Statistic name and the data.
export class StatisticLinkDereference<T> extends StatisticBase<T> {
  public fileLocation: string;
  public key: ActionContextKey<IStatisticBase<T>>;

  public constructor(fileLocation: string, statisticsToWrite: IStatisticBase<T>) {
    super();
    statisticsToWrite.on((data: T) => {
      this.updateStatistic(data)
    });
  }

  public updateStatistic(data: T): boolean {
    return true;
  }
}
