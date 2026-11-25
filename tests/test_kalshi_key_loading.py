from types import SimpleNamespace
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa

import app.feeds as feeds


def _ed25519_pem():
    key = ed25519.Ed25519PrivateKey.generate()
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return key, pem


def _rsa_pem():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return key, pem


def _hub_with_key_settings(monkeypatch, *, inline=None, path=None, key_id="test-key-id"):
    monkeypatch.setattr(
        feeds,
        "settings",
        SimpleNamespace(
            kalshi_key_id=key_id,
            kalshi_private_key=inline,
            kalshi_private_key_path=path,
        ),
    )
    return feeds.MarketDataHub()


def _public_bytes(key):
    return key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )


def test_loads_inline_ed25519_pem_with_real_newlines(monkeypatch):
    expected, pem = _ed25519_pem()
    hub = _hub_with_key_settings(monkeypatch, inline=pem.decode("utf-8"))

    assert isinstance(hub._private_key, ed25519.Ed25519PrivateKey)
    assert _public_bytes(hub._private_key) == _public_bytes(expected)
    assert hub.brti_detail == "Kalshi private key loaded from KALSHI_PRIVATE_KEY"


def test_loads_inline_ed25519_pem_with_escaped_newlines(monkeypatch):
    expected, pem = _ed25519_pem()
    inline = pem.decode("utf-8").replace("\n", r"\n")
    hub = _hub_with_key_settings(monkeypatch, inline=inline)

    assert isinstance(hub._private_key, ed25519.Ed25519PrivateKey)
    assert _public_bytes(hub._private_key) == _public_bytes(expected)


def test_loads_private_key_from_path_when_inline_value_is_absent(monkeypatch, tmp_path):
    expected, pem = _ed25519_pem()
    key_file = tmp_path / "kalshi-private-key.pem"
    key_file.write_bytes(pem)
    hub = _hub_with_key_settings(monkeypatch, path=str(key_file))

    assert isinstance(hub._private_key, ed25519.Ed25519PrivateKey)
    assert _public_bytes(hub._private_key) == _public_bytes(expected)
    assert hub.brti_detail == "Kalshi private key loaded from KALSHI_PRIVATE_KEY_PATH"


def test_falls_back_to_valid_path_when_inline_key_is_malformed(monkeypatch, tmp_path):
    expected, pem = _ed25519_pem()
    key_file = tmp_path / "kalshi-private-key.pem"
    key_file.write_bytes(pem)
    hub = _hub_with_key_settings(
        monkeypatch,
        inline="this-is-not-a-private-key",
        path=str(key_file),
    )

    assert isinstance(hub._private_key, ed25519.Ed25519PrivateKey)
    assert _public_bytes(hub._private_key) == _public_bytes(expected)
    assert hub.brti_detail == "Kalshi private key loaded from KALSHI_PRIVATE_KEY_PATH"


def test_accepts_rsa_private_key_from_path(monkeypatch, tmp_path):
    expected, pem = _rsa_pem()
    key_file = tmp_path / "kalshi-rsa-private-key.pem"
    key_file.write_bytes(pem)
    hub = _hub_with_key_settings(monkeypatch, path=str(key_file))

    assert isinstance(hub._private_key, rsa.RSAPrivateKey)
    assert hub._private_key.public_key().public_numbers() == expected.public_key().public_numbers()


def test_missing_key_id_does_not_attempt_to_load_key(monkeypatch):
    hub = _hub_with_key_settings(monkeypatch, inline="not-a-key", key_id=None)

    assert hub._private_key is None
    assert hub.brti_detail == "KALSHI_KEY_ID is not configured"


def test_malformed_inline_key_reports_source_without_exposing_key_contents(monkeypatch):
    bad_value = "SENSITIVE-TEST-KEY-MATERIAL"
    hub = _hub_with_key_settings(monkeypatch, inline=bad_value)

    assert hub._private_key is None
    assert "KALSHI_PRIVATE_KEY" in hub.brti_detail
    assert bad_value not in hub.brti_detail


def test_prefers_valid_inline_key_when_both_sources_are_configured(monkeypatch, tmp_path):
    inline_expected, inline_pem = _ed25519_pem()
    path_expected, path_pem = _ed25519_pem()
    key_file = tmp_path / "other-kalshi-private-key.pem"
    key_file.write_bytes(path_pem)

    hub = _hub_with_key_settings(
        monkeypatch,
        inline=inline_pem.decode("utf-8"),
        path=str(key_file),
    )

    assert isinstance(hub._private_key, ed25519.Ed25519PrivateKey)
    assert _public_bytes(hub._private_key) == _public_bytes(inline_expected)
    assert _public_bytes(hub._private_key) != _public_bytes(path_expected)
    assert hub.brti_detail == "Kalshi private key loaded from KALSHI_PRIVATE_KEY"

