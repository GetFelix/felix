// Argument handling for `connect` and `withIdentity`, including the app's
// token function made safe to call from the native side.

"use strict";

/**
 * Wrap the app's `tokenProvider` so the native side always gets a promise that
 * resolves with a non-empty string or rejects with an `Error`. A synchronous
 * throw, a rejection of something that is not an `Error`, and a result that is
 * not a string all become the same kind of failure, with a message the client
 * passes on unchanged.
 */
function fetchFrom(tokenProvider) {
  if (tokenProvider === undefined || tokenProvider === null) return undefined;
  if (typeof tokenProvider !== "function") {
    throw new TypeError("tokenProvider must be a function returning a string or a Promise of one");
  }
  return async function fetchToken() {
    let token;
    try {
      token = await tokenProvider();
    } catch (err) {
      throw err instanceof Error ? err : new Error(String(err));
    }
    if (typeof token !== "string" || token === "") {
      const got = typeof token === "string" ? "an empty string" : typeof token;
      throw new Error(`tokenProvider must return a non-empty string, got ${got}`);
    }
    return token;
  };
}

const CONNECT_OPTIONS = ["token", "tokenProvider", "serverName", "caFile", "offerAlpn"];
const IDENTITY_OPTIONS = ["token", "tokenProvider"];

/** The options object, checked: unknown keys are a mistake, not something to drop. */
function optionsOf(method, args, count, allowed) {
  if (args.length > count + 1) {
    throw new TypeError(`${method} takes ${count} arguments and an options object, got ${args.length}`);
  }
  const options = args[count] ?? {};
  if (typeof options !== "object" || Array.isArray(options)) {
    throw new TypeError(`${method}: the last argument must be an options object`);
  }
  const unknown = Object.keys(options).filter((key) => !allowed.includes(key));
  if (unknown.length > 0) {
    throw new TypeError(`${method}: unknown option ${unknown.join(", ")}; expected ${allowed.join(", ")}`);
  }
  return options;
}

/** `connect(addrs, tenantId, options)` as the addon's positional call. */
function connectArgs(args) {
  const o = optionsOf("Client.connect", args, 2, CONNECT_OPTIONS);
  return [args[0], args[1], o.token, o.serverName, o.caFile, o.offerAlpn, fetchFrom(o.tokenProvider)];
}

/** `withIdentity(tenantId, options)` as the addon's positional call. */
function withIdentityArgs(args) {
  const o = optionsOf("withIdentity", args, 1, IDENTITY_OPTIONS);
  return [args[0], o.token, fetchFrom(o.tokenProvider)];
}

module.exports = { fetchFrom, connectArgs, withIdentityArgs };
