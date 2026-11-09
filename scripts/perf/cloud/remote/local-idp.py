#!/usr/bin/env python3
"""A throwaway OIDC identity provider for a perf session: one RS256 key, its
JWKS document, and signed JWTs. Standard library plus the openssl CLI.

  local-idp.py init <dir>
      Create <dir>/key.pem (once) and write <dir>/www/jwks.json from it, so the
      directory to serve is <dir>/www and the private key stays out of it.
  local-idp.py mint <dir> <issuer> <audience> <subject> <ttl-seconds>
      Print a JWT signed with <dir>/key.pem.

The control plane (auth/oidc.rs) wants a `kid` header, RS256 or ES256, and the
claims iss, aud, sub, iat and exp.
"""
import base64
import hashlib
import json
import os
import subprocess
import sys
import time


def b64u(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=")


def openssl(*args, stdin=None):
    return subprocess.run(
        ["openssl", *args], input=stdin, check=True, capture_output=True
    ).stdout


def public_numbers(key_pem):
    """(n, e) of the key as big-endian bytes."""
    out = openssl("rsa", "-in", key_pem, "-noout", "-modulus").decode().strip()
    n = bytes.fromhex(out.split("=", 1)[1])
    return n, (65537).to_bytes(3, "big")


def kid_for(n):
    return hashlib.sha256(n).hexdigest()[:16]


def init(d):
    os.makedirs(d, exist_ok=True)
    key = os.path.join(d, "key.pem")
    if not os.path.exists(key):
        old = os.umask(0o077)
        try:
            openssl("genrsa", "-out", key, "2048")
        finally:
            os.umask(old)
    n, e = public_numbers(key)
    jwks = {
        "keys": [
            {
                "kty": "RSA",
                "kid": kid_for(n),
                "alg": "RS256",
                "use": "sig",
                "n": b64u(n).decode(),
                "e": b64u(e).decode(),
            }
        ]
    }
    os.makedirs(os.path.join(d, "www"), exist_ok=True)
    with open(os.path.join(d, "www", "jwks.json"), "w") as f:
        json.dump(jwks, f)


def mint(d, issuer, audience, subject, ttl):
    key = os.path.join(d, "key.pem")
    n, _ = public_numbers(key)
    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT", "kid": kid_for(n)}
    claims = {
        "iss": issuer,
        "sub": subject,
        "aud": audience,
        "iat": now,
        "exp": now + int(ttl),
    }
    signing_input = b".".join(
        b64u(json.dumps(part, separators=(",", ":")).encode())
        for part in (header, claims)
    )
    sig = openssl("dgst", "-sha256", "-sign", key, stdin=signing_input)
    return (signing_input + b"." + b64u(sig)).decode()


def main(argv):
    if len(argv) == 3 and argv[1] == "init":
        init(argv[2])
    elif len(argv) == 7 and argv[1] == "mint":
        print(mint(*argv[2:]))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv)
