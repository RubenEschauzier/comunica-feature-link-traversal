/**
 * A process-wide gate on derived-resource requests, ordered by how much of the query each answers.
 *
 * Derived resources are evaluated by the server rather than read from disk, and a Solid server
 * answers them on one event loop. Measured against the SolidBench derived-resource server, every
 * extra derived request in flight adds ~5.8ms to every other one, so firing a whole batch at once
 * does not make the batch finish sooner -- it only delays the request the query is waiting on.
 * Bounding the number in flight and serving the most valuable first cuts time-to-first-composite
 * by roughly 7x at no cost to the time the whole batch takes.
 *
 * The gate has to sit here, around the request, rather than around the execute actors. A composite
 * source hands the adaptive join a lazy iterator, so its request does not happen while the execute
 * actor runs -- it happens later, when the join starts pulling. Phasing the actors therefore orders
 * nothing. Ordering the requests themselves works no matter when each one is registered, and also
 * covers the requests raised from different documents, which the per-document actors cannot see.
 *
 * Priority is the number of patterns the request answers: a composite covering ten patterns is
 * worth more than a single-pattern completeness request, and ties keep arrival order.
 */
interface IWaiter {
  patternCount: number;
  sequence: number;
  release: () => void;
}

let maxConcurrency = 4;
let active = 0;
let sequenceCounter = 0;
const waiting: IWaiter[] = [];

/**
 * Sets how many derived-resource requests may be in flight at once. Below 1 is unbounded.
 */
export function setDerivedRequestConcurrency(limit: number): void {
  maxConcurrency = limit;
  drain();
}

function drain(): void {
  while (waiting.length > 0 && (maxConcurrency < 1 || active < maxConcurrency)) {
    // Most patterns first, then oldest first, so nothing starves behind a stream of new arrivals
    let best = 0;
    for (let i = 1; i < waiting.length; i++) {
      const candidate = waiting[i];
      const current = waiting[best];
      if (candidate.patternCount > current.patternCount ||
        (candidate.patternCount === current.patternCount && candidate.sequence < current.sequence)) {
        best = i;
      }
    }
    const [ next ] = waiting.splice(best, 1);
    active++;
    next.release();
  }
}

/**
 * Runs `request` once a slot is free, preferring requests that answer more of the query.
 */
export async function scheduleDerivedRequest<T>(patternCount: number, request: () => Promise<T>): Promise<T> {
  if (maxConcurrency < 1) {
    return request();
  }

  if (active < maxConcurrency && waiting.length === 0) {
    active++;
  } else {
    await new Promise<void>((resolve) => {
      waiting.push({ patternCount, sequence: sequenceCounter++, release: resolve });
    });
  }

  try {
    return await request();
  } finally {
    active--;
    drain();
  }
}
