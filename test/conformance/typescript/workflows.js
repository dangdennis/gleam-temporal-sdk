// The TypeScript twins of the workflows in test/conformance_test.gleam. Each pair must
// issue the same commands in the same order, so that a history recorded by either SDK
// replays on the other.
const wf = require('@temporalio/workflow');
const nexus = require('nexus-rpc');

const { double } = wf.proxyActivities({ startToCloseTimeout: '10 seconds' });
const { slow } = wf.proxyActivities({ startToCloseTimeout: '30 seconds' });
const added = wf.defineSignal('added');
const poke = wf.defineSignal('poke');
const finish = wf.defineSignal('finish');
const add = wf.defineUpdate('add');

// Timers, activities in sequence and in parallel, a signal, a child workflow.
exports.basics = async function basics(n) {
  const signals = [];
  wf.setHandler(added, (value) => void signals.push(value));
  await wf.sleep(10);
  const a = await double(n);
  const [b, c] = await Promise.all([double(a), double(a + 1)]);
  await wf.condition(() => signals.length > 0);
  const child = await wf.executeChild('double_child', {
    args: [c],
    workflowId: `${wf.workflowInfo().workflowId}-child`,
  });
  return b + c + signals[0] + child;
};

exports.double_child = async function double_child(n) {
  return await double(n);
};

// Cancellation: a deadline that cancels an activity, a scope that cancels a timer, and
// shielded cleanup after the workflow itself is cancelled.
exports.scopes = async function scopes(n) {
  try {
    await wf.CancellationScope.withTimeout(200, () => slow(2000));
  } catch (err) {
    if (!wf.isCancellation(err)) throw err;
  }
  const scope = new wf.CancellationScope();
  const pending = scope.run(() => wf.sleep(60_000)).catch(() => undefined);
  await wf.sleep(10);
  scope.cancel();
  await pending;
  try {
    await wf.sleep(60_000);
    return n;
  } catch (err) {
    await wf.CancellationScope.nonCancellable(() => double(n));
    throw err;
  }
};

// Waiting with a deadline: once for a signal that arrives, once for one that does not.
exports.deadline = async function deadline(n) {
  let pokes = 0;
  wf.setHandler(poke, () => void pokes++);
  const first = await wf.condition(() => pokes > 0, 60_000);
  const second = await wf.condition(() => pokes > 1, 50);
  return `${first} ${second} ${await double(n)}`;
};

// Updates: validated, answered after an activity, and one that fails after acceptance.
exports.updates = async function updates(start) {
  let total = start;
  let finished = false;
  wf.setHandler(finish, () => void (finished = true));
  wf.setHandler(
    add,
    async (amount) => {
      if (amount === 13) throw wf.ApplicationFailure.create({ message: 'unlucky', type: 'Unlucky' });
      total += await double(amount);
      return total;
    },
    {
      validator: (amount) => {
        if (amount < 0) throw new Error('amount must not be negative');
      },
    }
  );
  await wf.condition(() => finished);
  return total;
};

exports.listener = async function listener() {
  let text;
  wf.setHandler(poke, (value) => void (text = value));
  await wf.condition(() => text !== undefined);
  return `poked: ${text}`;
};

// Other workflows: signal one, cancel one, signal one that does not exist, then start a
// child and signal it.
exports.external = async function external(prefix) {
  await wf.getExternalWorkflowHandle(`${prefix}-signalled`).signal(poke, 'hello');
  await wf.getExternalWorkflowHandle(`${prefix}-cancelled`).cancel();
  let missing = false;
  try {
    await wf.getExternalWorkflowHandle(`${prefix}-missing`).signal(poke, 'anyone?');
  } catch (err) {
    missing = true;
  }
  const child = await wf.startChild('listener', { workflowId: `${prefix}-child` });
  await child.signal(poke, 'from parent');
  return `missing=${missing} ${await child.result()}`;
};

const local = wf.proxyLocalActivities({ startToCloseTimeout: '5 seconds' });
const retrying = wf.proxyLocalActivities({
  startToCloseTimeout: '5 seconds',
  localRetryThreshold: '1 second',
  retry: { initialInterval: '1500 milliseconds', backoffCoefficient: 1, maximumAttempts: 5 },
});

// Local activities: one that succeeds at once, and one whose retries are far enough apart
// that the wait becomes a timer in history.
exports.local = async function localActivities(attempts) {
  const doubled = await local.double(21);
  return `${doubled}, ${await retrying.stubborn(attempts)}`;
};

// Patching, memo changes and continue-as-new.
exports.lifecycle = async function lifecycle(remaining) {
  wf.upsertMemo({ stage: `remaining ${remaining}` });
  if (wf.patched('double-first')) {
    await double(remaining);
  } else {
    await wf.sleep(5);
  }
  wf.deprecatePatch('retired');
  await wf.sleep(10);
  if (remaining > 0) {
    await wf.continueAsNew(remaining - 1);
  }
  return 'done';
};

const text = nexus.service('text', {
  shout: nexus.operation(),
  refuse: nexus.operation(),
});
const maths = nexus.service('maths', { double: nexus.operation() });

// Nexus: a synchronous operation, a workflow-backed one, and one that fails.
exports.nexus_caller = async function nexusCaller(endpoint) {
  const texts = wf.createNexusServiceClient({ endpoint, service: text });
  const numbers = wf.createNexusServiceClient({ endpoint, service: maths });
  const shouted = await texts.executeOperation('shout', 'hello');
  const handle = await numbers.startOperation('double', 21);
  const doubled = await handle.result();
  let reason = 'not refused';
  try {
    await texts.executeOperation('refuse', 'this');
  } catch (err) {
    reason = 'refused';
  }
  return `${shouted}, ${doubled}, ${reason}`;
};

exports.doubling = async function doubling(n) {
  return await double(n);
};
