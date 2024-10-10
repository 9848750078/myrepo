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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const mockdate_1 = __importDefault(require("mockdate"));
const _1 = require(".");
describe("CreatePayload", () => {
    const region = "us-east-1";
    beforeAll(() => {
        mockdate_1.default.set("2021-01-01");
    });
    afterAll(() => {
        mockdate_1.default.reset();
    });
    describe("create", () => {
        it("should return correct authentication payload when credentials are an object", () => __awaiter(void 0, void 0, void 0, function* () {
            const credentials = {
                accessKeyId: "accessKeyId",
                sessionToken: "sessionToken",
                secretAccessKey: "secretAccessKey",
            };
            const authenticationPayloadCreator = new _1.CreatePayload({
                region,
                credentials,
            });
            const brokerHost = "examples.com";
            const payload = yield authenticationPayloadCreator.create({ brokerHost });
            expect(payload).toHaveProperty("version", "2020_10_22");
            expect(payload).toHaveProperty("host", brokerHost);
            expect(payload).toHaveProperty("user-agent", "MSK_IAM");
            expect(payload).toHaveProperty("action", "kafka-cluster:Connect");
            expect(payload).toHaveProperty("x-amz-algorithm", "AWS4-HMAC-SHA256");
            expect(payload).toHaveProperty("x-amz-credential", `accessKeyId/20210101/${region}/kafka-cluster/aws4_request`);
            expect(payload).toHaveProperty("x-amz-date", "20210101T000000Z");
            expect(payload).toHaveProperty("x-amz-security-token", "sessionToken");
            expect(payload).toHaveProperty("x-amz-signedheaders", "host");
            expect(payload).toHaveProperty("x-amz-expires", "900");
            expect(payload).toHaveProperty("x-amz-signature", "3515d158e62c159a3d8d4344942cff44c865cdeac6748a4b5dc42686641bb272");
        }));
        it("should return correct authentication payload when credentials are a method", () => __awaiter(void 0, void 0, void 0, function* () {
            const credentials = () => __awaiter(void 0, void 0, void 0, function* () {
                return ({
                    accessKeyId: "accessKeyId",
                    sessionToken: "sessionToken",
                    secretAccessKey: "secretAccessKey",
                });
            });
            const authenticationPayloadCreator = new _1.CreatePayload({
                region,
                credentials,
            });
            const brokerHost = "examples.com";
            const payload = yield authenticationPayloadCreator.create({ brokerHost });
            expect(payload).toHaveProperty("version", "2020_10_22");
            expect(payload).toHaveProperty("host", brokerHost);
            expect(payload).toHaveProperty("user-agent", "MSK_IAM");
            expect(payload).toHaveProperty("action", "kafka-cluster:Connect");
            expect(payload).toHaveProperty("x-amz-algorithm", "AWS4-HMAC-SHA256");
            expect(payload).toHaveProperty("x-amz-credential", `accessKeyId/20210101/${region}/kafka-cluster/aws4_request`);
            expect(payload).toHaveProperty("x-amz-date", "20210101T000000Z");
            expect(payload).toHaveProperty("x-amz-security-token", "sessionToken");
            expect(payload).toHaveProperty("x-amz-signedheaders", "host");
            expect(payload).toHaveProperty("x-amz-expires", "900");
            expect(payload).toHaveProperty("x-amz-signature", "3515d158e62c159a3d8d4344942cff44c865cdeac6748a4b5dc42686641bb272");
        }));
    });
});
