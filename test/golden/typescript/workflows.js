// The TypeScript twin of `golden_workflow` in test/golden_test.gleam. The two must issue
// the same commands in the same order.
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
