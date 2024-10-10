"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createSaslAuthenticationRequest = void 0;
const _1 = require(".");
/** @internal */
const createSaslAuthenticationRequest = (payload) => ({
    encode: () => {
        const stringifiedPayload = JSON.stringify(payload);
        const byteLength = Buffer.byteLength(stringifiedPayload, "utf8");
        const buf = Buffer.alloc(_1.INT32_SIZE + byteLength);
        buf.writeUInt32BE(byteLength, 0);
        buf.write(stringifiedPayload, _1.INT32_SIZE, byteLength, "utf8");
        return buf;
    },
});
exports.createSaslAuthenticationRequest = createSaslAuthenticationRequest;
