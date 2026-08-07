import { IDynamicFilter } from '@comunica/types-link-traversal';
import { minimatch } from 'minimatch'

export class DynamicFilter implements IDynamicFilter {
  private readonly exact = new Set<string>();
  private readonly globs = new Set<string>();
  private readonly globsArray: string[] = [];

  public addExact(exactMatch: string){
    this.exact.add(exactMatch);
  }

  public addGlob(selector: string): void {
    const glob = selector.replace(/\.[^./*]+$/, '');

    if (!this.globs.has(glob)) {
      this.globs.add(glob);
      this.globsArray.push(glob);
    }
  }

  public matchesFilter(url: string): boolean {
    if (this.exact.has(url)) {
      return true;
    }
    return this.globsArray.some((glob) => minimatch(url, glob));
  }
}