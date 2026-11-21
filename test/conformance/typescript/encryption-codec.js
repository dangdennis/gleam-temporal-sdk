// The codec the TypeScript half of the tests encrypts with, and its shared test key.
const crypto = require('crypto');
const { temporal } = require('@temporalio/proto');

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

module.exports = { encryptionCodec };
