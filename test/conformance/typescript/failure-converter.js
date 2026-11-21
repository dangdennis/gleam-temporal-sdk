// The TypeScript SDK's failure converter with message and stack trace moved into the
// failure's encoded attributes, for `failureConverterPath`. This SDK's equivalent is
// `client.with_encoded_failure_attributes`.
const { DefaultFailureConverter } = require('@temporalio/common');

module.exports = { failureConverter: new DefaultFailureConverter({ encodeCommonAttributes: true }) };
