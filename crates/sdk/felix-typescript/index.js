// The package's JavaScript half: load the addon, and give its errors an
// identity a caller can branch on. The error classes and how a native error
// becomes one are in `errors.js`.

"use strict";

const { existsSync } = require("node:fs");
const { join } = require("node:path");

const errors = require("./errors.js");

const { typed } = errors;

/**
 * What this machine's binary is called, in napi's naming.
 *
 * It names both the platform package (`felix-client-linux-x64-gnu`) and the
 * file inside it (`felix.linux-x64-gnu.node`), so the two cannot drift.
 */
function platformTag() {
  const { platform, arch } = process;
  if (platform === "linux") {
    // A glibc build will not load on musl. `glibcVersionRuntime` is absent on
    // musl, which is the only reliable check from inside Node — reading
    // `process.report` costs nothing and is not gated on a flag.
    let libc = "musl";
    try {
      if (process.report?.getReport()?.header?.glibcVersionRuntime) libc = "gnu";
    } catch {
      // A locked-down runtime can refuse the report. Assume glibc, which is
      // what is published; the load below fails with a clear message if wrong.
      libc = "gnu";
    }
    return `linux-${arch}-${libc}`;
  }
  if (platform === "win32") return `win32-${arch}-msvc`;
  return `${platform}-${arch}`;
}

function loadAddon() {
  const tag = platformTag();

  // The repository shares one target directory across every crate, this one
  // included (`.cargo/config.toml`), so a development build lands at the root
  // rather than beside this file.
  const roots = [join(__dirname, "target"), join(__dirname, "..", "..", "..", "target")];
  const names = [
    "libfelix_typescript.dylib",
    "libfelix_typescript.so",
    "felix_typescript.dll",
  ];
  // `napi build --platform` writes the tagged name; a plain `napi build` the
  // bare one. Both are checked before the installed package, so a local
  // rebuild wins over whatever npm put in node_modules.
  const candidates = [
    join(__dirname, `felix.${tag}.node`),
    join(__dirname, "felix.node"),
  ];
  // A plain `cargo build` is enough to use this package, which is what keeps it
  // usable without the napi CLI — `napi build` is mostly a rename.
  for (const profile of ["release", "debug"]) {
    for (const root of roots) {
      for (const name of names) candidates.push(join(root, profile, name));
    }
  }
  for (const path of candidates) {
    if (!existsSync(path)) continue;
    if (path.endsWith(".node")) return require(path);
    // Node only `require`s files named `.node`, but `process.dlopen` — which
    // is what `require` calls underneath — takes any path. That is what lets a
    // plain `cargo build` be enough, with no rename step.
    const shim = { exports: {} };
    process.dlopen(shim, path);
    return shim.exports;
  }

  // An installed package has none of the above: npm ships one package per
  // platform and this package declares them all as optional dependencies, so
  // exactly the matching one is present.
  const pkg = `felix-client-${tag}`;
  try {
    return require(pkg);
  } catch (err) {
    // MODULE_NOT_FOUND here means this platform has no published binary, which
    // is worth saying plainly — the alternative is a stack trace about a
    // package the caller never named.
    if (err?.code !== "MODULE_NOT_FOUND") throw err;
  }

  throw new Error(
    `felix-client: no native addon for ${tag}. Either this platform has no ` +
      `published binary, or the optional dependency ${pkg} did not install. ` +
      `From a checkout, build it with \`napi build --release\` or ` +
      `\`cargo build --release\` in crates/sdk/felix-typescript.`,
  );
}

const native = loadAddon();

/**
 * Whether `value` is one of the addon's own classes.
 *
 * Only those get wrapped. Everything else a method resolves with — a Buffer, an
 * array of records, a plain object — is the caller's data, and a Proxy around
 * it is not that data: `deepStrictEqual` sees through to the handler,
 * `Buffer.concat` and other brand checks can reject it, and the caller has no
 * way to unwrap. Wrapping only the handles keeps the typed-error layer on the
 * calls, where it belongs, and leaves the payloads alone.
 */
function isHandle(value) {
  const constructor = value?.constructor;
  return typeof constructor === "function" && native[constructor.name] === constructor;
}

/**
 * Wrap a native handle so every rejection arrives typed.
 *
 * A Proxy rather than patching the prototype: napi defines its methods
 * non-configurable, so `defineProperty` refuses. Proxying also means a method
 * added to the Rust side is covered without being listed here — nothing to
 * forget.
 *
 * `Symbol.asyncDispose` is added on top so `await using` releases a handle on
 * every path out of a block, including a throw. It resolves to `close` when
 * the instance has one, and to nothing when it does not.
 */
function wrap(value) {
  if (!isHandle(value)) return value;
  return new Proxy(value, {
    get(target, prop) {
      if (prop === Symbol.asyncDispose) {
        if (typeof target.close !== "function") return undefined;
        return async function dispose() {
          await target.close();
        };
      }
      const member = Reflect.get(target, prop, target);
      if (typeof member !== "function") return member;
      return function called(...args) {
        try {
          const out = member.apply(target, args);
          if (out && typeof out.then === "function") {
            return out.then(wrap, (err) => Promise.reject(typed(err)));
          }
          return wrap(out);
        } catch (err) {
          throw typed(err);
        }
      };
    },
  });
}

/**
 * Builders for the ops `client.commit` takes. Each returns a plain object, so
 * writing the object literal by hand works just as well.
 */
const CommitOp = {
  publish: (stream, payload) => ({ op: "publish", stream, payload: Buffer.from(payload) }),
  enqueue: (queue, payload) => ({ op: "enqueue", queue, payload: Buffer.from(payload) }),
  put: (stream, key, value) => ({ op: "put", stream, key, value: Buffer.from(value) }),
  delete: (stream, key) => ({ op: "delete", stream, key }),
};

/** The public `Client`: a façade over the native one that types its errors. */
const Client = {
  async connect(addrs, tenantId, token, serverName, caFile, offerAlpn) {
    try {
      const client = await native.Client.connect(addrs, tenantId, token, serverName, caFile, offerAlpn);
      return wrap(client);
    } catch (err) {
      throw typed(err);
    }
  },
};

module.exports = {
  Client,
  FelixError: errors.FelixError,
  ConnectionError: errors.ConnectionError,
  AuthError: errors.AuthError,
  NotFoundError: errors.NotFoundError,
  CursorError: errors.CursorError,
  InvalidArgumentError: errors.InvalidArgumentError,
  ShardUnavailableError: errors.ShardUnavailableError,
  OverloadedError: errors.OverloadedError,
  OutcomeUnknownError: errors.OutcomeUnknownError,
  CommitError: errors.CommitError,
  NotOnOwningShardError: errors.NotOnOwningShardError,
  EventCountError: errors.EventCountError,
  CommitOp,
  /** The unwrapped addon, for anyone who wants it. Errors are untyped there. */
  native,
};
