// The app's token function, made safe to call from the native side.

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

module.exports = { fetchFrom };
