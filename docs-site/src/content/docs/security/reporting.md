---
title: "Reporting a Vulnerability"
description: "How to report a security problem in Felix privately, what to include, and what happens next."
---

Report it privately through
[GitHub Security Advisories](https://github.com/GetFelix/felix/security/advisories/new).
Please do not open a public issue, pull request or discussion, and do not push
a public fix branch before the advisory is published: a fix commit is a
disclosure.

If you cannot use GitHub Security Advisories, contact the maintainer
([@gabloe](https://github.com/gabloe)) privately and ask for a channel. Leave
the details out of that first message.

## What to include

- The version: a release tag, a commit, or an image digest.
- The component, such as the broker, the control plane, the wire protocol or
  a client.
- What an attacker gains, and where they start from: a network peer with no
  credential, a token for another tenant, a tenant admin, a broker or
  control-plane machine, or disk access. The [threat model](/security/threat-model/)
  uses the same actors.
- A way to reproduce it. A small client, a raw frame, a `curl` call or a
  failing test is ideal.
- The configuration that matters, for example whether bootstrap or peer mTLS
  is on.

## What happens next

| Stage | Target |
| --- | --- |
| Acknowledgement | 3 business days |
| Initial assessment | 10 business days |
| Fix or documented mitigation for a confirmed high-severity issue | 90 days from acknowledgement |

These are targets for a small project, not a contract. A confirmed issue gets
a private fix, a published advisory (with a CVE where one applies), a patch
release, and a correction to these pages if they claimed something the code
could not back. Reporters are credited unless they would rather not be.
Felix has no bug bounty.

Gaps already listed in the [threat model](/security/threat-model/) and the
[overview](/security/#not-built) are known. A new concrete attack they enable
in a deployment that follows [Hardening a deployment](/security/hardening/) is
still worth reporting.

The full policy, including scope, supported versions and safe harbor for
good-faith research, is
[SECURITY.md](https://github.com/GetFelix/felix/blob/main/SECURITY.md).
