// Records the histories of one run of each workflow on the official TypeScript SDK and writes them,
// protobuf-encoded, into the directory given as the first argument. Needs a Temporal service on
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
    activities: {
      double: async (n) => n * 2,
      slow: async (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    },
  });
  const client = new Client();
  const save = async (handle, name) => {
    const history = await handle.fetchHistory();
    const path = `${process.argv[2]}/${name}`;
    fs.writeFileSync(path, temporal.api.history.v1.History.encode(history).finish());
  };
  await worker.runUntil(async () => {
    const golden = await client.workflow.start('golden', {
      taskQueue,
      workflowId: 'golden-typescript',
      args: [5],
    });
    await golden.signal('added', 1000);
    console.log(`golden result ${await golden.result()}`);
    await save(golden, 'typescript_sdk_history.bin');

    // Cancelled twice over: once after the workflow has reached its final sleep, and once
    // early, while a scope's timer and a plain timer are both pending. The dev server
    // fires timers about a second late, which is what makes 1.5 seconds early.
    for (const [name, wait] of [['scopes', 5000], ['early_cancel', 1500]]) {
      const handle = await client.workflow.start('scopes', {
        taskQueue,
        workflowId: `${name}-typescript`,
        args: [5],
      });
      await new Promise((resolve) => setTimeout(resolve, wait));
      await handle.cancel();
      await handle.result().catch((error) => console.log(`${name} ended: ${error.cause?.name}`));
      await save(handle, `typescript_sdk_${name}_history.bin`);
    }
  });
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
