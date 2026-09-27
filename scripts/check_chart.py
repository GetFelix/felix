#!/usr/bin/env python3
"""Render the Helm chart every way the CI value sets describe, and check the
result says what the chart promises.

`helm lint` catches YAML that does not parse and values the schema rejects.
This adds the claims a reader of the chart is relying on: the drain fits the
grace period, the budgets keep a quorum, the internal port is never on a
Service, secrets are referenced and never rendered, and the combinations the
templates are supposed to refuse are in fact refused.

Needs `helm` on PATH and PyYAML.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

try:
    import yaml
except ImportError:  # pragma: no cover - environment, not logic
    sys.exit("check_chart.py needs PyYAML (pip install pyyaml)")

ROOT = pathlib.Path(__file__).resolve().parent.parent
CHART = ROOT / "deploy" / "helm" / "felix"
RELEASE = "felix"
NAMESPACE = "felix-test"

failures: list[str] = []


def fail(message: str) -> None:
    failures.append(message)
    print(f"  FAIL {message}")


def helm(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["helm", *args], capture_output=True, text=True, check=False
    )


def render(values: pathlib.Path | None, *sets: str) -> list[dict]:
    args = ["template", RELEASE, str(CHART), "--namespace", NAMESPACE]
    if values is not None:
        args += ["--values", str(values)]
    for item in sets:
        args += ["--set", item]
    result = helm(*args)
    if result.returncode != 0:
        fail(f"helm template failed for {values or 'defaults'} {sets}:\n{result.stderr}")
        return []
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def must_refuse(reason: str, values: pathlib.Path | None, *sets: str) -> None:
    args = ["template", RELEASE, str(CHART), "--namespace", NAMESPACE]
    if values is not None:
        args += ["--values", str(values)]
    for item in sets:
        args += ["--set", item]
    result = helm(*args)
    if result.returncode == 0:
        fail(f"rendered a release that should have been refused ({reason}): {sets}")
    elif reason not in result.stderr:
        fail(f"refused {sets}, but not for the expected reason ({reason!r}):\n{result.stderr}")
    else:
        print(f"  refused as expected: {reason}")


def by_kind(docs: list[dict], kind: str) -> list[dict]:
    return [doc for doc in docs if doc.get("kind") == kind]


def one(docs: list[dict], kind: str, name_suffix: str) -> dict | None:
    for doc in by_kind(docs, kind):
        if doc["metadata"]["name"].endswith(name_suffix):
            return doc
    return None


def containers(workload: dict) -> list[dict]:
    return workload["spec"]["template"]["spec"]["containers"]


def env_of(container: dict) -> dict[str, dict]:
    return {entry["name"]: entry for entry in container.get("env", [])}


def check_broker(docs: list[dict], label: str) -> None:
    sts = one(docs, "StatefulSet", "-broker")
    if sts is None:
        fail(f"{label}: no broker StatefulSet")
        return
    spec = sts["spec"]["template"]["spec"]
    broker = containers(sts)[0]
    env = env_of(broker)

    # Identity: the pod name is the node id, and peers get an IP.
    if env.get("FELIX_NODE_ID", {}).get("value") != "$(POD_NAME)":
        fail(f"{label}: FELIX_NODE_ID is not the pod name")
    advertise = env.get("FELIX_NODE_ADVERTISE_ADDR", {}).get("value", "")
    if not advertise.startswith("$(POD_IP):"):
        fail(f"{label}: FELIX_NODE_ADVERTISE_ADDR is {advertise!r}, not the pod IP")

    # The handoff and the drain fit inside the grace period, with the preStop
    # sleep before them.
    pre_stop = spec["containers"][0]["lifecycle"]["preStop"]["exec"]["command"][-1]
    sleep = int(pre_stop.split()[-1])
    handoff_ms = int(env["FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS"]["value"])
    drain_ms = int(env["FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS"]["value"])
    grace = int(spec["terminationGracePeriodSeconds"])
    if grace < sleep + (handoff_ms + drain_ms) // 1000:
        fail(
            f"{label}: grace {grace}s < preStop {sleep}s + handoff {handoff_ms}ms"
            f" + drain {drain_ms}ms"
        )

    # The credential is a Secret reference, never a value.
    for entry in broker.get("env", []):
        value = entry.get("value", "")
        if "TOKEN" in entry["name"] and value and not value.startswith("/etc/felix/"):
            fail(f"{label}: {entry['name']} carries a literal value")
    volumes = {volume["name"]: volume for volume in spec["volumes"]}
    if "secret" not in volumes.get("credential", {}):
        fail(f"{label}: the credential volume is not a Secret")

    # Nothing runs as root or can write its own image.
    security = broker["securityContext"]
    if security.get("readOnlyRootFilesystem") is not True or "ALL" not in security["capabilities"]["drop"]:
        fail(f"{label}: broker container is not locked down")
    if spec["securityContext"].get("runAsNonRoot") is not True:
        fail(f"{label}: broker pod may run as root")

    # The internal port is on the headless Service only, never a routable one.
    internal_port = int(env["FELIX_INTERNAL_BIND"]["value"].rsplit(":", 1)[1])
    for service in by_kind(docs, "Service"):
        if service["spec"].get("clusterIP") == "None":
            continue
        for port in service["spec"]["ports"]:
            if port["port"] == internal_port and port.get("protocol") == "UDP":
                fail(f"{label}: Service {service['metadata']['name']} exposes the internal port")

    # The budget keeps a replication-factor-three quorum.
    pdb = one(docs, "PodDisruptionBudget", "-broker")
    replicas = int(sts["spec"]["replicas"])
    if pdb is not None:
        if int(pdb["spec"]["maxUnavailable"]) >= replicas:
            fail(f"{label}: broker PDB permits every broker to go at once")
        if replicas >= 3 and int(pdb["spec"]["maxUnavailable"]) > 1:
            fail(f"{label}: broker PDB permits two replicas of one shard to go at once")

    # The network policy, when present, admits the internal port from brokers only.
    policy = one(docs, "NetworkPolicy", "-broker")
    if policy is not None:
        for rule in policy["spec"]["ingress"]:
            ports = {port["port"] for port in rule.get("ports", [])}
            if internal_port in ports:
                sources = rule.get("from", [])
                if len(sources) != 1 or "podSelector" not in sources[0]:
                    fail(f"{label}: the internal port is admitted from more than the brokers")

    # Peer mTLS wires all three files or none.
    tls_env = [name for name in env if name.startswith("FELIX_INTERNAL_TLS_")]
    if tls_env and len(tls_env) != 3:
        fail(f"{label}: peer TLS is half-configured: {tls_env}")
    if tls_env and "csi" not in volumes.get("peer-tls", {}):
        fail(f"{label}: peer TLS is set but no per-pod certificate volume is mounted")
    # A broker refuses to start with neither peer mTLS nor the explicit opt-out.
    if not tls_env and env.get("FELIX_INTERNAL_ALLOW_UNAUTHENTICATED", {}).get("value") != "true":
        fail(f"{label}: no peer mTLS and no FELIX_INTERNAL_ALLOW_UNAUTHENTICATED; brokers would not start")

    # A configured client certificate comes from a mounted Secret, and a
    # broker that lost it refuses rather than generating one.
    if "FELIX_TLS_CERT" in env:
        if "secret" not in volumes.get("client-tls", {}):
            fail(f"{label}: FELIX_TLS_CERT is set but no client-tls Secret is mounted")
        if env.get("FELIX_TLS_REQUIRE_CERT", {}).get("value") != "true":
            fail(f"{label}: a configured client certificate is not required")
        if "FELIX_TLS_CERT_EXPORT" in env:
            fail(f"{label}: FELIX_TLS_CERT_EXPORT set with FELIX_TLS_CERT; the broker refuses that")

    # An https control plane with a private CA hands brokers the CA.
    url = env.get("FELIX_CONTROLPLANE_URL", {}).get("value", "")
    if "FELIX_CONTROLPLANE_CA" in env and not url.startswith("https://"):
        fail(f"{label}: FELIX_CONTROLPLANE_CA with {url!r}; the broker refuses that")


def check_controlplane(docs: list[dict], label: str, backend: str) -> None:
    kind = "StatefulSet" if backend == "raft" else "Deployment"
    workload = one(docs, kind, "-controlplane")
    if workload is None:
        fail(f"{label}: no control-plane {kind}")
        return
    container = containers(workload)[0]
    env = env_of(container)
    if env["FELIX_CONTROLPLANE_STORAGE_BACKEND"]["value"] != backend:
        fail(f"{label}: backend is not {backend}")

    if backend == "postgres":
        url = env.get("FELIX_CONTROLPLANE_POSTGRES_URL", {})
        if "valueFrom" not in url or "secretKeyRef" not in url["valueFrom"]:
            fail(f"{label}: the Postgres URL is not a Secret reference")
        strategy = workload["spec"]["strategy"]["rollingUpdate"]
        if strategy["maxUnavailable"] != 0:
            fail(f"{label}: control-plane instances do not roll one at a time")
    if backend == "raft":
        replicas = int(workload["spec"]["replicas"])
        peers = env["FELIX_RAFT_PEERS"]["value"].split(",")
        if len(peers) != replicas:
            fail(f"{label}: {len(peers)} peers for {replicas} members")
        ids = [entry.split("=")[0] for entry in peers]
        if ids != [str(i + 1) for i in range(replicas)]:
            fail(f"{label}: peer ids are {ids}")
        if "FELIX_RAFT_NODE_ID" not in "".join(container["command"]):
            fail(f"{label}: no member derives its Raft id from its ordinal")
        if not workload["spec"].get("volumeClaimTemplates"):
            fail(f"{label}: Raft members have no volume")
        headless = one(docs, "Service", "-controlplane-headless")
        if headless is None or not headless["spec"].get("publishNotReadyAddresses"):
            fail(f"{label}: Raft members cannot find each other before they are ready")
        # The Raft routes can replace the whole store: their own port, never
        # the API one, and a peer token from a Secret.
        api_port = next(
            p["containerPort"] for p in container["ports"] if p["name"] == "api"
        )
        peer_ports = [p["containerPort"] for p in container["ports"] if p["name"] == "raft"]
        if len(peer_ports) != 1 or peer_ports[0] == api_port:
            fail(f"{label}: Raft members have no peer port of their own")
        bind = env.get("FELIX_RAFT_BIND_ADDR", {}).get("value", "")
        if peer_ports and not bind.endswith(f":{peer_ports[0]}"):
            fail(f"{label}: FELIX_RAFT_BIND_ADDR {bind!r} is not the peer port")
        if peer_ports and any(not entry.endswith(f":{peer_ports[0]}") for entry in peers):
            fail(f"{label}: the peers map does not name the peer port")
        token = env.get("FELIX_RAFT_PEER_TOKEN", {})
        if "secretKeyRef" not in token.get("valueFrom", {}):
            fail(f"{label}: the Raft peer token is not a Secret reference")
        if not env.get("FELIX_RAFT_CLUSTER_ID", {}).get("value"):
            fail(f"{label}: no Raft cluster id")
        # Empty members may form a group only until the bootstrap hook has
        # seen it formed and left its marker; every start reads the marker.
        formed = env.get("FELIX_RAFT_GROUP_FORMED", {}).get("valueFrom", {}).get("configMapKeyRef", {})
        marker = one(docs, "ConfigMap", "-controlplane-raft-formed")
        job = one(docs, "Job", "-controlplane-raft-bootstrap")
        if "FELIX_RAFT_INITIAL_CLUSTER_STATE" in env:
            fail(f"{label}: the initial cluster state is fixed in the pod template")
        if not formed.get("optional") or marker is None or formed.get("name") != marker["metadata"]["name"]:
            fail(f"{label}: members do not read the group-formed marker, optionally")
        script = "".join(container["command"])
        if "FELIX_RAFT_INITIAL_CLUSTER_STATE=existing" not in script:
            fail(f"{label}: a member that sees the marker does not start as existing")
        if job is None or marker is None:
            fail(f"{label}: no one-shot Raft bootstrap hook")
        else:
            weight = lambda doc: int(doc["metadata"]["annotations"]["helm.sh/hook-weight"])
            if "post-install" not in job["metadata"]["annotations"].get("helm.sh/hook", ""):
                fail(f"{label}: the bootstrap Job is not a post-install hook")
            if weight(marker) <= weight(job):
                fail(f"{label}: the group-formed marker is not created after the bootstrap Job")
            if "/v1/system/ready" not in "".join(containers(job)[0]["command"]):
                fail(f"{label}: the bootstrap Job does not wait for the group to be ready")
        policy = one(docs, "NetworkPolicy", "-controlplane")
        if policy is None:
            fail(f"{label}: nothing keeps the Raft peer port to the members")
        elif peer_ports:
            rules = [rule for rule in policy["spec"]["ingress"]
                     if any(p["port"] == peer_ports[0] for p in rule.get("ports", []))]
            if len(rules) != 1 or not rules[0].get("from"):
                fail(f"{label}: the Raft peer port is open to every pod")
        if headless is not None and [p["name"] for p in headless["spec"]["ports"]] != ["raft"]:
            fail(f"{label}: the headless Service does not expose the peer port")

    # A TLS API is probed over HTTPS, from a mounted Secret.
    if "FELIX_CONTROLPLANE_TLS_CERT" in env:
        for probe in ("livenessProbe", "readinessProbe"):
            if container[probe]["httpGet"].get("scheme") != "HTTPS":
                fail(f"{label}: {probe} is not HTTPS against a TLS API")
        spec_volumes = {v["name"]: v for v in workload["spec"]["template"]["spec"]["volumes"]}
        if "secret" not in spec_volumes.get("tls", {}):
            fail(f"{label}: the API certificate is not a mounted Secret")

    # Liveness never asks the store; readiness does.
    probes = container
    if probes["livenessProbe"]["httpGet"]["path"] != "/v1/system/live":
        fail(f"{label}: liveness is not /v1/system/live")
    if probes["readinessProbe"]["httpGet"]["path"] != "/v1/system/ready":
        fail(f"{label}: readiness is not /v1/system/ready")

    bootstrap_token = env.get("FELIX_BOOTSTRAP_TOKEN")
    if bootstrap_token is not None:
        if "secretKeyRef" not in bootstrap_token.get("valueFrom", {}):
            fail(f"{label}: the bootstrap token is not a Secret reference")
        api = one(docs, "Service", "-controlplane")
        bootstrap_port = int(env["FELIX_BOOTSTRAP_BIND_ADDR"]["value"].rsplit(":", 1)[1])
        if api is not None and any(port["port"] == bootstrap_port for port in api["spec"]["ports"]):
            fail(f"{label}: the bootstrap listener is on the API Service")
        if one(docs, "Service", "-controlplane-bootstrap") is None:
            fail(f"{label}: bootstrap is on but has no Service of its own")


def check_no_secret_material(docs: list[dict], label: str) -> None:
    for doc in docs:
        if doc.get("kind") == "Secret":
            fail(f"{label}: the chart rendered a Secret ({doc['metadata']['name']}); it must only reference them")
        if doc.get("kind") == "ConfigMap":
            text = yaml.safe_dump(doc.get("data", {}))
            if "postgres://" in text or "TOKEN" in text.upper():
                fail(f"{label}: ConfigMap {doc['metadata']['name']} carries a credential")


def lint(values: pathlib.Path | None) -> None:
    args = ["lint", "--strict", str(CHART), "--namespace", NAMESPACE]
    if values is not None:
        args += ["--values", str(values)]
    result = helm(*args)
    if result.returncode != 0:
        fail(f"helm lint failed for {values or 'defaults'}:\n{result.stdout}{result.stderr}")


def main() -> int:
    ci = CHART / "ci"
    postgres = ci / "postgres-values.yaml"
    raft = ci / "raft-values.yaml"
    mtls = ci / "mtls-values.yaml"
    memory = ci / "memory-values.yaml"

    print("lint")
    for values in (postgres, raft, mtls, memory):
        lint(values)

    print("render: postgres")
    docs = render(postgres)
    check_controlplane(docs, "postgres", "postgres")
    check_broker(docs, "postgres")
    check_no_secret_material(docs, "postgres")
    if one(docs, "ConfigMap", "-broker") is None:
        fail("postgres: broker.config did not render a ConfigMap")
    if len(by_kind(docs, "ServiceMonitor")) != 2:
        fail("postgres: expected a ServiceMonitor per workload")

    print("render: raft")
    docs = render(raft)
    check_controlplane(docs, "raft", "raft")
    check_broker(docs, "raft")
    check_no_secret_material(docs, "raft")
    sts = one(docs, "StatefulSet", "-broker")
    env = env_of(containers(sts)[0]) if sts else {}
    if env.get("FELIX_NODE_TOKEN_FILE", {}).get("value") != "/etc/felix/credential/$(POD_NAME)":
        fail("raft: per-broker credentials do not read a file named after the pod")
    if "FELIX_NODE_REFRESH_TOKEN_FILE" not in env:
        fail("raft: the refresh token was not wired")

    print("render: raft peer TLS")
    docs = render(raft, "controlplane.storage.raft.tls.enabled=true",
                  "controlplane.storage.raft.tls.existingSecret=raft-peer-tls")
    check_controlplane(docs, "raft-tls", "raft")
    sts = one(docs, "StatefulSet", "-controlplane")
    env = env_of(containers(sts)[0]) if sts else {}
    if not all(env.get(f"FELIX_RAFT_TLS_{k}", {}).get("value", "").startswith("/etc/felix/raft-tls/")
               for k in ("CERT", "KEY", "CA")):
        fail("raft-tls: FELIX_RAFT_TLS_* not wired to the mounted Secret")
    volumes = {v["name"]: v for v in sts["spec"]["template"]["spec"]["volumes"]} if sts else {}
    if volumes.get("raft-tls", {}).get("secret", {}).get("secretName") != "raft-peer-tls":
        fail("raft-tls: the peer certificate is not a mounted Secret")

    print("render: raft with an explicit initial cluster state")
    docs = render(raft, "controlplane.storage.raft.initialClusterState=existing")
    sts = one(docs, "StatefulSet", "-controlplane")
    env = env_of(containers(sts)[0]) if sts else {}
    if env.get("FELIX_RAFT_INITIAL_CLUSTER_STATE", {}).get("value") != "existing":
        fail("raft-existing: an explicit initial cluster state was not kept")
    if one(docs, "Job", "-controlplane-raft-bootstrap") is not None:
        fail("raft-existing: the bootstrap hook ran although the state was explicit")

    print("render: mtls")
    docs = render(mtls)
    check_controlplane(docs, "mtls", "postgres")
    check_broker(docs, "mtls")
    check_no_secret_material(docs, "mtls")
    sts = one(docs, "StatefulSet", "-broker")
    if sts and int(sts["spec"]["template"]["spec"]["terminationGracePeriodSeconds"]) != 120:
        fail("mtls: an explicit grace period that fits was not kept")
    env = env_of(containers(sts)[0]) if sts else {}
    if env.get("FELIX_CLIENT_ADVERTISE_ADDR", {}).get("value") != "$(POD_NAME).brokers.example.com:5000":
        fail("mtls: the external client address was not used")
    if env.get("FELIX_TLS_CLIENT_CA", {}).get("value") != "/etc/felix/client-tls/clients-ca.crt":
        fail("mtls: the client CA was not wired")
    if not env.get("FELIX_CONTROLPLANE_URL", {}).get("value", "").startswith("https://"):
        fail("mtls: brokers do not reach a TLS control plane over https")
    if "FELIX_CONTROLPLANE_CA" not in env:
        fail("mtls: brokers are not given the control plane's CA")

    print("render: memory (brokers off)")
    docs = render(memory)
    check_controlplane(docs, "memory", "memory")
    if by_kind(docs, "StatefulSet"):
        fail("memory: brokers rendered while disabled")
    check_no_secret_material(docs, "memory")

    print("render: defaults with the two required secrets")
    docs = render(
        None,
        "controlplane.storage.postgres.existingSecret=pg",
        "broker.credential.existingSecret=cred",
        "broker.peerTls.allowUnauthenticated=true",
    )
    check_controlplane(docs, "defaults", "postgres")
    check_broker(docs, "defaults")

    print("refusals")
    must_refuse("broker.credential.existingSecret is empty", None,
                "controlplane.storage.postgres.existingSecret=pg")
    must_refuse("brokers refuse to start without peer mTLS", None,
                "controlplane.storage.postgres.existingSecret=pg",
                "broker.credential.existingSecret=cred")
    must_refuse("needs broker.clientTls.existingSecret", postgres,
                "broker.clientTls.enabled=true")
    must_refuse("needs controlplane.tls.existingSecret", postgres,
                "controlplane.tls.enabled=true")

    must_refuse("needs controlplane.storage.postgres.existingSecret", None,
                "broker.credential.existingSecret=cred")
    must_refuse("odd number of members", raft, "controlplane.replicas=4")
    must_refuse("at least three members", raft, "controlplane.replicas=1")
    must_refuse("needs controlplane.storage.raft.peerToken.existingSecret", raft,
                "controlplane.storage.raft.peerToken.existingSecret=")
    must_refuse("the Raft RPCs need a listener of their own", raft,
                "controlplane.storage.raft.peerPort=8443")
    must_refuse("needs controlplane.storage.raft.tls.existingSecret", raft,
                "controlplane.storage.raft.tls.enabled=true")
    must_refuse("memory keeps metadata in one process", memory, "controlplane.replicas=2")
    must_refuse("share a port", postgres, "broker.ports.internal=5000")
    must_refuse("permit evicting every broker", postgres,
                "broker.replicas=1", "broker.podDisruptionBudget.maxUnavailable=1")
    must_refuse("loses its quorum when two", postgres,
                "broker.podDisruptionBudget.maxUnavailable=2")
    must_refuse("no voluntary disruption could ever proceed", postgres,
                "controlplane.podDisruptionBudget.minAvailable=2")
    must_refuse("SIGKILL would arrive mid-drain", postgres,
                "broker.shutdown.terminationGracePeriodSeconds=30")
    must_refuse("needs controlplane.bootstrap.existingSecret", postgres,
                "controlplane.bootstrap.enabled=true")
    must_refuse("cannot advertise a control plane", postgres,
                "controlplane.enabled=false")
    must_refuse("values don't meet the specifications of the schema", postgres,
                "controlplane.storage.backend=sqlite")
    must_refuse("values don't meet the specifications of the schema", postgres,
                "broker.replicaCount=3")

    if failures:
        print(f"\n{len(failures)} chart check(s) failed")
        return 1
    print("\nchart checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
