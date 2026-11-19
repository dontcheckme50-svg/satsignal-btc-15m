import types

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

import app.feeds as feeds_module
from app.feeds import MarketDataHub


def _pem():
    key = ed25519.Ed25519PrivateKey.generate()
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def test_load_kalshi_key_from_inline_pem_secret(monkeypatch):
    pem = _pem().decode("utf-8")
    monkeypatch.setattr(
        feeds_module,
        "settings",
        types.SimpleNamespace(
            kalshi_key_id="test-key-id",
            kalshi_private_key=pem.replace("\n", "\\n"),
            kalshi_private_key_path=None,
        ),
    )
    hub = MarketDataHub.__new__(MarketDataHub)
    hub.brti_detail = ""

    loaded = hub._load_kalshi_key()

    assert isinstance(loaded, ed25519.Ed25519PrivateKey)


def test_load_kalshi_key_from_mounted_pem_file(monkeypatch, tmp_path):
    path = tmp_path / "kalshi.pem"
    path.write_bytes(_pem())
    monkeypatch.setattr(
        feeds_module,
        "settings",
        types.SimpleNamespace(
            kalshi_key_id="test-key-id",
            kalshi_private_key=None,
            kalshi_private_key_path=str(path),
        ),
    )
    hub = MarketDataHub.__new__(MarketDataHub)
    hub.brti_detail = ""

    loaded = hub._load_kalshi_key()

    assert isinstance(loaded, ed25519.Ed25519PrivateKey)


def test_missing_kalshi_private_key_reports_configuration_hint(monkeypatch):
    monkeypatch.setattr(
        feeds_module,
        "settings",
        types.SimpleNamespace(
            kalshi_key_id="test-key-id",
            kalshi_private_key=None,
            kalshi_private_key_path=None,
        ),
    )
    hub = MarketDataHub.__new__(MarketDataHub)
    hub.brti_detail = ""

    assert hub._load_kalshi_key() is None
    assert "KALSHI_PRIVATE_KEY_PATH" in hub.brti_detail
