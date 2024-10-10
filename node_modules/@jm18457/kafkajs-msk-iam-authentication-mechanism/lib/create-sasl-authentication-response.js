"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createSaslAuthenticationResponse = void 0;
const _1 = require(".");
/** @internal */
exports.createSaslAuthenticationResponse = {
    decode: (rawData) => {
        const byteLength = rawData.readInt32BE(0);
        return rawData.slice(_1.INT32_SIZE, _1.INT32_SIZE + byteLength);
    },
    parse: (data) => {
        return JSON.parse(data.toString());
    },
};
