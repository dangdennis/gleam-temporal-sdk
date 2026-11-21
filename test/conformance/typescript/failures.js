// The TypeScript half of the encoded-failure interop check in
// test/failure_privacy_test.gleam.
//
//   node failures.js <gleam task queue> <typescript task queue> <secret>
//
// Both sides encrypt payloads with the shared test key and encode failure messages and
// stack traces (`encodeCommonAttributes` here). Runs a TypeScript worker whose activity
// fails, for a workflow of this SDK to call and pass the failure on, and with the
// TypeScript client runs two of this SDK's workflows that fail and reads why. Prints one
// line of JSON.
const { Worker, DefaultLogger, Runtime, NativeConnection } = require('@temporalio/worker');
const { Client, Connection } = require('@temporalio/client');
const { ApplicationFailure } = require('@temporalio/common');
const { encryptionCodec } = require('./encryption-codec');

Runtime.install({ logger: new DefaultLogger('ERROR') });

const dataConverter = {
  payloadCodecs: [encryptionCodec],
  failureConverterPath: require.resolve('./failure-converter'),
};

// The messages of a failure and its causes, outermost first.
function messages(error) {
  return error ? [error.message, ...messages(error.cause)] : [];
}

function innermost(error) {
  return error.cause ? innermost(error.cause) : error;
}

async function main() {
  const [gleamQueue, typescriptQueue, secret] = process.argv.slice(2);
  const connection = await Connection.connect();
  const client = new Client({ connection, dataConverter });
  const nativeConnection = await NativeConnection.connect();
  const worker = await Worker.create({
    connection: nativeConnection,
    taskQueue: typescriptQueue,
    dataConverter,
    activities: {
      ts_decline: async (card) => {
        throw ApplicationFailure.create({ message: `typescript declined ${card}`, type: 'Declined', nonRetryable: true });
      },
    },
  });

  const failed = (workflow, args) =>
    client.workflow.execute(workflow, { taskQueue: gleamQueue, workflowId: `${gleamQueue}-${workflow}`, args }).then(
      () => ({ messages: ['it did not fail'] }),
      (error) => ({ messages: messages(error.cause), type: innermost(error).type, stack: innermost(error).stack.length > 0 })
    );
  const out = await worker.runUntil(async () => ({
    // An activity of this SDK fails; its workflow passes the failure on.
    charging: await failed('charging', [secret]),
    // The TypeScript activity fails; a workflow of this SDK passes the failure on.
    relaying: await failed('relaying', [typescriptQueue, secret]),
    // A workflow of this SDK fails with a failure of its own.
    refusing: await failed('refusing', [secret]),
  }));
  await nativeConnection.close();
  await connection.close();
  console.log(JSON.stringify(out));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
