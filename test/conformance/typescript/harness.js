// The TypeScript half of the conformance harness.
//
//   node harness.js record <dir> [feature...]
//                                  run every twin, or the named ones, on the official
//                                  TypeScript SDK and write each history,
//                                  protobuf-encoded, to <dir>/<feature>.bin
//   node harness.js replay <dir>   replay the histories this SDK's tests wrote to <dir>
//                                  against the TypeScript twins; exits 1 if any diverges
//
// Recording needs a Temporal service on localhost:7233 and the `temporal` CLI.
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execSync } = require('child_process');
const { Worker, DefaultLogger, Runtime } = require('@temporalio/worker');
const { Client, Connection } = require('@temporalio/client');
const { temporal } = require('@temporalio/proto');
const nexus = require('nexus-rpc');
const { WorkflowRunOperationHandler, startWorkflow } = require('@temporalio/nexus');

Runtime.install({ logger: new DefaultLogger('ERROR') });

const workflowsPath = require.resolve('./workflows');
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// The shared test key and the codec of Temporal's encryption sample: AES-256-GCM over the
// protobuf-encoded payload, nonce first.
const keyId = 'conformance';
const key = Buffer.alloc(32, 7);
const encryptionCodec = {
  async encode(payloads) {
    return payloads.map((payload) => {
      const nonce = crypto.randomBytes(12);
      const cipher = crypto.createCipheriv('aes-256-gcm', key, nonce);
      const plain = temporal.api.common.v1.Payload.encode(payload).finish();
      const data = Buffer.concat([nonce, cipher.update(plain), cipher.final(), cipher.getAuthTag()]);
      return {
        metadata: { encoding: Buffer.from('binary/encrypted'), 'encryption-key-id': Buffer.from(keyId) },
        data,
      };
    });
  },
  async decode(payloads) {
    return payloads.map((payload) => {
      if (Buffer.from(payload.metadata?.encoding ?? []).toString() !== 'binary/encrypted') return payload;
      const data = Buffer.from(payload.data);
      const decipher = crypto.createDecipheriv('aes-256-gcm', key, data.subarray(0, 12));
      decipher.setAuthTag(data.subarray(data.length - 16));
      const plain = Buffer.concat([decipher.update(data.subarray(12, data.length - 16)), decipher.final()]);
      return temporal.api.common.v1.Payload.decode(plain);
    });
  },
};
const encrypted = { payloadCodecs: [encryptionCodec] };

const activities = {
  double: async (n) => n * 2,
  slow: async (ms) => sleep(ms),
  join: async (text, times) => Array(times).fill(text).join(' '),
  maybe: async (present) => (present ? 'value' : undefined),
  nothing: async () => {},
  stubborn: async (attempts) => {
    const { Context } = require('@temporalio/activity');
    const attempt = Context.current().info.attempt;
    if (attempt < attempts) throw new Error('not yet');
    return `attempt ${attempt}`;
  },
};

const text = nexus.service('text', { shout: nexus.operation(), refuse: nexus.operation() });
const maths = nexus.service('maths', { double: nexus.operation() });

function nexusServices(taskQueue) {
  return [
    nexus.serviceHandler(text, {
      shout: async (_ctx, input) => input.toUpperCase(),
      refuse: async (_ctx, input) => {
        throw new nexus.OperationError('failed', `refused ${input}`);
      },
    }),
    nexus.serviceHandler(maths, {
      double: new WorkflowRunOperationHandler(async (ctx, n) =>
        startWorkflow(ctx, 'doubling', { args: [n], workflowId: `nexus-double-${ctx.requestId}`, taskQueue })
      ),
    }),
  ];
}

// How each feature is driven once its workflow has started. Timings need not match the
// other SDK's driver: only the workflow code has to agree.
const features = {
  basics: {
    args: [5],
    drive: async (handle) => handle.signal('added', 1000),
  },
  scopes: {
    args: [5],
    // Long enough for the workflow to reach its final sleep.
    drive: async (handle) => {
      await sleep(5000);
      await handle.cancel();
    },
  },
  early_cancel: {
    workflow: 'scopes',
    args: [5],
    // Depends on the cancellation landing at one moment; only its TypeScript history is kept.
    typescriptOnly: true,
    // The dev server fires timers about a second late, so this lands while a scope's
    // timer and a plain timer are both pending.
    drive: async (handle) => {
      await sleep(1500);
      await handle.cancel();
    },
  },
  deadline: {
    args: [4],
    drive: async (handle) => handle.signal('poke'),
  },
  updates: {
    args: [100],
    drive: async (handle) => {
      await handle.executeUpdate('add', { args: [1] });
      await handle.executeUpdate('add', { args: [-5] }).catch(() => undefined);
      await handle.executeUpdate('add', { args: [13] }).catch(() => undefined);
      await handle.executeUpdate('add', { args: [10] });
      await handle.signal('finish');
    },
  },
  external: {
    args: ({ workflowId }) => [workflowId],
    before: async ({ client, taskQueue, workflowId }) => {
      for (const suffix of ['signalled', 'cancelled']) {
        await client.workflow.start('listener', { taskQueue, workflowId: `${workflowId}-${suffix}` });
      }
    },
  },
  local: { args: [3] },
  arguments: { args: ['hey', 3, true] },
  // The second argument is left out.
  optional: { args: ['hey'] },
  handover: { args: [1] },
  lifecycle: { args: [2] },
  nexus_caller: {
    args: ({ endpoint }) => [endpoint],
    nexus: true,
  },
  encrypted: {
    workflow: 'basics',
    args: [5],
    dataConverter: encrypted,
    drive: async (handle) => handle.signal('added', 1000),
  },
};

async function record(dir, only) {
  fs.mkdirSync(dir, { recursive: true });
  const connection = await Connection.connect();
  for (const [name, feature] of Object.entries(features)) {
    if (only.length > 0 && !only.includes(name)) continue;
    const taskQueue = `conformance-ts-${name}-${Date.now()}`;
    const workflowId = `${name}-typescript`;
    const endpoint = `conformance-ts-${Date.now()}`;
    if (feature.nexus) {
      execSync(
        `temporal operator nexus endpoint create --name ${endpoint} --target-namespace default --target-task-queue ${taskQueue}`
      );
    }
    const dataConverter = feature.dataConverter;
    const client = new Client({ connection, dataConverter });
    // A fixed id is reused across recordings: clear the way for this one.
    await client.workflow.getHandle(workflowId).terminate().catch(() => undefined);
    const worker = await Worker.create({
      taskQueue,
      workflowsPath,
      activities,
      dataConverter,
      nexusServices: feature.nexus ? nexusServices(taskQueue) : undefined,
    });
    const context = { client, taskQueue, workflowId, endpoint };
    await worker.runUntil(async () => {
      if (feature.before) await feature.before(context);
      const args = typeof feature.args === 'function' ? feature.args(context) : feature.args;
      const handle = await client.workflow.start(feature.workflow ?? name, { taskQueue, workflowId, args });
      if (feature.drive) await feature.drive(handle);
      const outcome = await handle.result().then(
        (value) => `returned ${JSON.stringify(value)}`,
        (error) => `ended with ${error.cause?.name ?? error.name}`
      );
      // `result` follows continue-as-new; the fixture is the first run's history.
      const first = client.workflow.getHandle(workflowId, handle.firstExecutionRunId);
      const history = await first.fetchHistory();
      fs.writeFileSync(path.join(dir, `${name}.bin`), temporal.api.history.v1.History.encode(history).finish());
      console.log(`${name}: ${outcome}, ${history.events.length} events`);
    });
  }
  await connection.close();
}

async function replayOne(history, dataConverter) {
  // Workflows derive ids from their own, so replay under the id the run really had.
  const workflowId = history.events[0].workflowExecutionStartedEventAttributes.workflowId;
  await Worker.runReplayHistory({ workflowsPath, dataConverter }, history, workflowId);
}

// Replays what the Gleam SDK recorded and prints the conformance table. The first three
// columns are asserted by test/conformance_test.gleam, which has to have passed for the
// histories in <dir> to exist; the last column is checked here.
async function replay(dir) {
  const read = (file) => temporal.api.history.v1.History.decode(fs.readFileSync(file));
  const recorded = fs.existsSync(dir) ? fs.readdirSync(dir).filter((name) => name.endsWith('.bin')) : [];
  const fixtures = fs.readdirSync(path.join(__dirname, '../../fixtures/conformance'));
  const names = [...new Set([...fixtures, ...recorded])].map((file) => path.basename(file, '.bin')).sort();
  let failed = 0;
  const rows = [];
  for (const name of names) {
    let reverse = 'not recorded';
    if (recorded.includes(`${name}.bin`)) {
      try {
        await replayOne(read(path.join(dir, `${name}.bin`)), features[name]?.dataConverter);
        reverse = 'replays';
      } catch (error) {
        failed++;
        reverse = 'DIVERGES';
        console.error(`${name}: ${error.message.split('\n')[0]}`);
      }
    } else if (features[name]?.typescriptOnly) {
      reverse = 'n/a';
    } else {
      failed++;
    }
    rows.push([name, fixtures.includes(`${name}.bin`) ? 'replays' : 'MISSING', 'rejected', reverse === 'n/a' ? 'n/a' : 'both', reverse]);
  }

  // A control: the replayer must refuse a history paired with the wrong workflow.
  let control = 'FAILED: a mismatched history replayed';
  if (recorded.includes('updates.bin')) {
    const history = read(path.join(dir, 'updates.bin'));
    history.events[0].workflowExecutionStartedEventAttributes.workflowType.name = 'basics';
    await replayOne(history).catch(() => (control = 'ok'));
  }
  if (control !== 'ok') failed++;

  const header = ['feature', 'TypeScript history on Gleam', 'diverging twin', 'Gleam cache modes', 'Gleam history on TypeScript'];
  const widths = header.map((title, i) => Math.max(title.length, ...rows.map((row) => row[i].length)));
  const line = (cells) => cells.map((cell, i) => cell.padEnd(widths[i])).join('  ');
  console.log(line(header));
  console.log(widths.map((width) => '-'.repeat(width)).join('  '));
  rows.forEach((row) => console.log(line(row)));
  console.log(`\nTypeScript replayer control (mismatched history is refused): ${control}`);
  process.exit(failed === 0 ? 0 : 1);
}

const [mode, dir, ...only] = process.argv.slice(2);
(mode === 'record' ? record(dir, only) : replay(dir)).catch((error) => {
  console.error(error);
  process.exit(1);
});
