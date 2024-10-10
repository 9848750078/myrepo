"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createAuthenticator = void 0;
const _1 = require(".");
const createAuthenticator = (options) => (args) => ({
    authenticate: () => __awaiter(void 0, void 0, void 0, function* () {
        const { host, port, logger, saslAuthenticate } = args;
        const broker = `${host}:${port}`;
        const payloadFactory = new _1.CreatePayload(options);
        try {
            const payload = yield payloadFactory.create({ brokerHost: host });
            const authenticateResponse = yield saslAuthenticate({
                request: (0, _1.createSaslAuthenticationRequest)(payload),
                response: _1.createSaslAuthenticationResponse,
            });
            logger.info("Authentication response", { authenticateResponse });
            const isValidResponse = authenticateResponse &&
                typeof authenticateResponse === "object" &&
                "version" in authenticateResponse &&
                authenticateResponse.version;
            if (!isValidResponse) {
                throw new Error("Invalid response from broker");
            }
            logger.info(`SASL ${_1.TYPE} authentication successful`, { broker });
        }
        catch (error) {
            if (error instanceof Error) {
                logger.error(error === null || error === void 0 ? void 0 : error.message, { broker });
            }
            else if (typeof error === "string") {
                logger.error(error, { broker });
            }
            throw error;
        }
    }),
});
exports.createAuthenticator = createAuthenticator;
