const { spawn } = require('child_process');
const path = require('path');

const QUERIES = [
  { name: 'query-remote-d-1-1.txt', path: 'query-remote-d-1-1.txt' },
  { name: 'query-remote.txt', path: 'query-remote.txt' },
];

function runQuery(queryFile) {
  return new Promise((resolve, reject) => {
    const startTime = performance.now();
    let firstResultTime = null;
    let lastResultTime = null;
    let resultCount = 0;
    let stdoutBuffer = '';
    let stderrBuffer = '';

    const binPath = path.join(__dirname, 'engines/query-sparql-link-traversal-solid/bin/query.js');
    const child = spawn('node', [binPath, '-f', queryFile, '--idp', 'void', '--lenient'], {
      cwd: __dirname,
    });

    const timeout = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error(`Query execution timed out after 25 seconds`));
    }, 25000);

    child.stdout.on('data', (chunk) => {
      const text = chunk.toString();
      stdoutBuffer += text;

      // Check for JSON binding results in stdout (e.g. lines containing {"..."})
      const lines = text.split('\n');
      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed.startsWith('{') && trimmed.includes(':')) {
          const now = performance.now() - startTime;
          if (firstResultTime === null) {
            firstResultTime = now;
          }
          lastResultTime = now;
          resultCount++;
        }
      }
    });

    child.stderr.on('data', (chunk) => {
      stderrBuffer += chunk.toString();
    });

    child.on('error', (err) => {
      clearTimeout(timeout);
      reject(err);
    });

    child.on('close', (code) => {
      clearTimeout(timeout);
      const totalTime = performance.now() - startTime;
      if (code !== 0) {
        return reject(new Error(`Process exited with code ${code}: ${stderrBuffer}`));
      }

      // If results were parsed as a single JSON array at the end
      if (resultCount === 0) {
        try {
          const cleaned = stdoutBuffer.substring(stdoutBuffer.indexOf('['), stdoutBuffer.lastIndexOf(']') + 1);
          if (cleaned) {
            const parsed = JSON.parse(cleaned);
            if (Array.isArray(parsed)) {
              resultCount = parsed.length;
            }
          }
        } catch (e) {
          // ignore
        }
      }

      resolve({
        query: queryFile,
        firstResultTime: firstResultTime !== null ? `${firstResultTime.toFixed(1)} ms` : 'N/A',
        lastResultTime: lastResultTime !== null ? `${lastResultTime.toFixed(1)} ms` : 'N/A',
        totalTime: `${totalTime.toFixed(1)} ms`,
        resultCount,
      });
    });
  });
}

async function main() {
  console.log('='.repeat(75));
  console.log('  Running Performance Benchmark for Derived Resources Query Execution');
  console.log('='.repeat(75));

  const results = [];
  for (const q of QUERIES) {
    console.log(`\nExecuting: ${q.name}...`);
    try {
      const res = await runQuery(q.path);
      results.push(res);
    } catch (err) {
      console.error(`Error running ${q.name}:`, err.message);
      results.push({
        query: q.name,
        firstResultTime: 'ERROR',
        lastResultTime: 'ERROR',
        totalTime: 'ERROR',
        resultCount: 0,
      });
    }
  }

  console.log('\n' + '='.repeat(75));
  console.log('  Benchmark Summary');
  console.log('='.repeat(75));
  console.table(results);
}

main();
