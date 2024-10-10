"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createMechanism = void 0;
const _1 = require(".");
const createMechanism = (options, mechanism = _1.TYPE) => ({
    mechanism,
    authenticationProvider: (0, _1.createAuthenticator)(options),
});
exports.createMechanism = createMechanism;
