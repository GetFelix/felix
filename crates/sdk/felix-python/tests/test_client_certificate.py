"""Client certificates: validation without a broker, and mutual TLS against one.

The fixture's mutual TLS broker serves only a client that presents the
certificate it names, as a broker started with FELIX_TLS_CLIENT_CA does.
"""

from __future__ import annotations

import asyncio
import pathlib

import pytest

import felix


def _connect(addrs="127.0.0.1:1", **options):
    return felix.Client(addrs, tenant_id="t", token="x", **options)


@pytest.fixture
def pem_files(tmp_path: pathlib.Path) -> dict[str, str]:
    notpem = tmp_path / "not-pem.pem"
    notpem.write_text("this is not a certificate\n")
    empty = tmp_path / "empty.pem"
    empty.write_text("")
    return {
        "notpem": str(notpem),
        "empty": str(empty),
        "missing": str(tmp_path / "missing.pem"),
    }


@pytest.mark.parametrize("given", ["client_cert_file", "client_key_file"])
def test_a_certificate_needs_its_key_and_the_key_its_certificate(given, pem_files):
    with pytest.raises(ValueError, match="must be given together"):
        _connect(**{given: pem_files["notpem"]})


def test_a_missing_certificate_file_is_named(pem_files):
    with pytest.raises(ValueError, match="missing.pem"):
        _connect(
            client_cert_file=pem_files["missing"], client_key_file=pem_files["notpem"]
        )


def test_a_file_without_a_certificate_is_named(pem_files):
    for name in ("empty", "notpem"):
        with pytest.raises(ValueError, match=pathlib.Path(pem_files[name]).name):
            _connect(
                client_cert_file=pem_files[name], client_key_file=pem_files["notpem"]
            )


def test_the_async_client_validates_too(pem_files):
    async def connect():
        await felix.AsyncClient.connect(
            "127.0.0.1:1", tenant_id="t", token="x", client_cert_file=pem_files["notpem"]
        )

    with pytest.raises(ValueError, match="must be given together"):
        asyncio.run(connect())


def _mutual_tls(fixture):
    return {
        "addrs": fixture["mtls_addr"],
        "tenant_id": fixture["tenant_id"],
        "token": fixture["token"],
        "ca_file": fixture["ca_file"],
        "reconnect_attempts": 1,
    }


def test_a_client_with_a_certificate_connects_to_a_broker_that_requires_one(fixture):
    with felix.Client(
        client_cert_file=fixture["client_cert_file"],
        client_key_file=fixture["client_key_file"],
        **_mutual_tls(fixture),
    ):
        pass


def test_a_client_without_a_certificate_is_refused_by_it(fixture):
    with pytest.raises(felix.FelixError):
        felix.Client(**_mutual_tls(fixture))


def test_the_async_client_presents_a_certificate_too(fixture):
    async def connect(**extra):
        client = await felix.AsyncClient.connect(**_mutual_tls(fixture), **extra)
        await client.close()

    asyncio.run(
        connect(
            client_cert_file=fixture["client_cert_file"],
            client_key_file=fixture["client_key_file"],
        )
    )
    with pytest.raises(felix.FelixError):
        asyncio.run(connect())
