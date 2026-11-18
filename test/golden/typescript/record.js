// Records the history of one `golden` run on the official TypeScript SDK and writes it,
// protobuf-encoded, to the path given as the first argument. Needs a Temporal service on
// localhost:7233.
const fs = require('fs');
const { Worker } = require('@temporalio/worker');
const { Client } = require('@temporalio/client');
const { temporal } = require('@temporalio/proto');

async function main() {
  const taskQueue = `golden-ts-${Date.now()}`;
  const worker = await Worker.create({
    taskQueue,
    workflowsPath: require.resolve('./workflows'),
    activities: { double: async (n) => n * 2 },
  });
  const client = new Client();
  const result = await worker.runUntil(async () => {
    const handle = await client.workflow.start('golden', {
      taskQueue,
      workflowId: 'golden-typescript',
      args: [5],
    });
    await handle.signal('added', 1000);
    const result = await handle.result();
    const history = await handle.fetchHistory();
    fs.writeFileSync(process.argv[2], temporal.api.history.v1.History.encode(history).finish());
    return result;
  });
  console.log(`result ${result}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
