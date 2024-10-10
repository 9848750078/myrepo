"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ACTION = exports.ALGORITHM = exports.HASHED_PAYLOAD = exports.SIGNED_HEADERS = exports.SERVICE = exports.TYPE = exports.INT32_SIZE = void 0;
/** @internal */
exports.INT32_SIZE = 4;
exports.TYPE = "AWS_MSK_IAM";
/** @internal */
exports.SERVICE = "kafka-cluster";
/** @internal */
exports.SIGNED_HEADERS = "host";
/** @internal */
exports.HASHED_PAYLOAD = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/** @internal */
exports.ALGORITHM = "AWS4-HMAC-SHA256";
/** @internal */
exports.ACTION = "kafka-cluster:Connect";
