// Convert a history in Temporal's JSON form to the protobuf form the replayer reads.
const fs = require('fs');
const { historyFromJSON } = require('@temporalio/common/lib/proto-utils');
const { temporal } = require('@temporalio/proto');
const [input, output] = process.argv.slice(2);
let json = JSON.parse(fs.readFileSync(input, 'utf8'));
if (Array.isArray(json)) json = json[0];
const history = historyFromJSON(json);
fs.writeFileSync(output, temporal.api.history.v1.History.encode(history).finish());
console.log(`${output}: ${history.events.length} events`);
