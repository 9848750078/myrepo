export * from "./constants";
export * from "./create-authenticator";
export * from "./create-mechanism";
export * from "./create-payload";
export * from "./create-sasl-authentication-request";
export * from "./create-sasl-authentication-response";
export declare const Type = "AWS_MSK_IAM";
export declare const awsIamAuthenticator: (options: import("./create-mechanism").Options) => (args: import("kafkajs").AuthenticationProviderArgs) => import("kafkajs").Authenticator;
