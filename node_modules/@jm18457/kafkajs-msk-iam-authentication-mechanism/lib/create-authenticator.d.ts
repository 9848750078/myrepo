import { AuthenticationProviderArgs, Authenticator } from "kafkajs";
import { Options } from ".";
export declare const createAuthenticator: (options: Options) => (args: AuthenticationProviderArgs) => Authenticator;
