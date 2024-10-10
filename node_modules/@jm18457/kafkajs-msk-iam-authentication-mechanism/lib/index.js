"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.awsIamAuthenticator = exports.Type = void 0;
__exportStar(require("./constants"), exports);
__exportStar(require("./create-authenticator"), exports);
__exportStar(require("./create-mechanism"), exports);
__exportStar(require("./create-payload"), exports);
__exportStar(require("./create-sasl-authentication-request"), exports);
__exportStar(require("./create-sasl-authentication-response"), exports);
const create_authenticator_1 = require("./create-authenticator");
const constants_1 = require("./constants");
exports.Type = constants_1.TYPE;
exports.awsIamAuthenticator = create_authenticator_1.createAuthenticator;
