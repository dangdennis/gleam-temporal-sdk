// The TypeScript half of the standalone-activity interop check in
// test/standalone_activity_test.gleam.
//
//   node standalone.js <gleam task queue> <typescript task queue> <activity id>
//
// Starts a standalone activity with the official TypeScript client for a worker of this
// SDK to run, and runs a TypeScript worker until the standalone activity this SDK's
// client started for it, <activity id>, has finished. Prints one line of JSON.
const { Worker, DefaultLogger, Runtime, NativeConnection } = require('@temporalio/worker');
const { Client, Connection } = require('@temporalio/client');
const { Context } = require('@temporalio/activity');

Runtime.install({ logger: new DefaultLogger('ERROR') });

async function main() {
  const [gleamQueue, typescriptQueue, activityId] = process.argv.slice(2);
  const connection = await Connection.connect();
  const client = new Client({ connection });

  const seen = {};
  const nativeConnection = await NativeConnection.connect();
  const worker = await Worker.create({
    connection: nativeConnection,
    taskQueue: typescriptQueue,
    activities: {
      triple: async (n) => {
        const info = Context.current().info;
        seen.activityId = info.activityId;
        seen.inWorkflow = info.inWorkflow;
        return n * 3;
      },
    },
  });

  const out = await worker.runUntil(async () => {
    const fromGleamWorker = await client.activity.execute('double', {
      id: `${gleamQueue}-from-typescript`,
      taskQueue: gleamQueue,
      args: [21],
      startToCloseTimeout: '10s',
      retry: { maximumAttempts: 1 },
    });
    const byGleamClient = await client.activity.getHandle(activityId).result();
    return { fromGleamWorker, byGleamClient, seen };
  });
  await nativeConnection.close();
  await connection.close();
  console.log(JSON.stringify(out));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
