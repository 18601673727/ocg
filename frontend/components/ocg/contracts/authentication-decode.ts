import type { AuthenticationMode, AuthenticationSession } from "./generated";
import { bad, boolean, decode, index, nullable, record, req, string, yes, type Decoder } from "./decode";

const mode: Decoder<AuthenticationMode> = (input, path) =>
  input === "local" || input === "cloudflare-access" ? yes(input) : bad(path, "an authentication mode");

const session: Decoder<AuthenticationSession> = (input, path) => {
  const value = record(input, path, "an authentication session");
  if (!value.ok) return value;
  const authentication = req(value.value, "mode", mode, path);
  if (!authentication.ok) return authentication;
  const user = req(value.value, "user_id", nullable(string), path);
  if (!user.ok) return user;
  const expiration = req(value.value, "expires_at", nullable(index), path);
  if (!expiration.ok) return expiration;
  const execution = req(value.value, "remote_execution", boolean, path);
  if (!execution.ok) return execution;
  const email = req(value.value, "email", nullable(string), path);
  if (!email.ok) return email;
  return yes({ mode: authentication.value, user_id: user.value, expires_at: expiration.value, remote_execution: execution.value, email: email.value });
};

export const decodeAuthenticationSession = (input: unknown): AuthenticationSession => decode((value) => session(value, "session"), input);
