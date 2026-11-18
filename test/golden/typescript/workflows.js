// The TypeScript twins of the workflows in test/golden_test.gleam. Each pair must issue the
// same commands in the same order.
const wf = require('@temporalio/workflow');

const { double } = wf.proxyActivities({ startToCloseTimeout: '10 seconds' });
const added = wf.defineSignal('added');

exports.golden = async function golden(n) {
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

const { slow } = wf.proxyActivities({ startToCloseTimeout: '30 seconds' });

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
