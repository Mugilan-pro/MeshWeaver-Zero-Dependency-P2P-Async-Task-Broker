"""
MeshWeaver TLS/SSL Subsystem
Provides end-to-end transport layer security for node-to-node TCP communication.
Automatically generates self-signed X.509 certificates and provisions SSL contexts
for encrypted task dispatch and result streams.
"""

from __future__ import annotations

import datetime
import logging
import os
import ssl
import tempfile
from typing import Optional, Tuple

logger = logging.getLogger("MeshWeaver.Security.TLS")

try:
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    _HAS_X509 = True
except ImportError:
    _HAS_X509 = False


def generate_self_signed_cert(
    common_name: str = "localhost",
    valid_days: int = 365,
    key_size: int = 2048,
) -> Tuple[bytes, bytes]:
    """
    Generates a self-signed X.509 certificate and private key in PEM format.
    Returns (cert_pem_bytes, key_pem_bytes).
    """
    if not _HAS_X509:
        raise RuntimeError("The 'cryptography' library is required to generate TLS certificates.")

    # 1. Generate RSA private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
    )

    # 2. Build Certificate
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MeshWeaver Decentralized Network"),
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
    ])

    now = datetime.datetime.utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=valid_days))
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        .sign(private_key, hashes.SHA256())
    )

    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return cert_pem, key_pem


def create_server_ssl_context(
    cert_pem: bytes,
    key_pem: bytes,
) -> ssl.SSLContext:
    """
    Creates an SSLContext configured for an asyncio TLS server.
    Writes cert and key to secure temporary files for the standard library SSL loader.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    # Require TLS 1.2 minimum
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2

    with tempfile.NamedTemporaryFile("wb", delete=False, suffix=".crt") as f_cert:
        f_cert.write(cert_pem)
        cert_path = f_cert.name

    with tempfile.NamedTemporaryFile("wb", delete=False, suffix=".key") as f_key:
        f_key.write(key_pem)
        key_path = f_key.name

    try:
        ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
    finally:
        try:
            os.remove(cert_path)
        except OSError:
            pass
        try:
            os.remove(key_path)
        except OSError:
            pass

    return ctx


def create_client_ssl_context(
    ca_cert_pem: Optional[bytes] = None,
    verify_peer: bool = False,
) -> ssl.SSLContext:
    """
    Creates an SSLContext configured for an asyncio TLS client.
    For self-signed peer mesh networks without a centralized CA, peer verification
    can be disabled (verify_peer=False) while retaining complete wire encryption.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2

    if not verify_peer:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    elif ca_cert_pem:
        with tempfile.NamedTemporaryFile("wb", delete=False, suffix=".crt") as f_ca:
            f_ca.write(ca_cert_pem)
            ca_path = f_ca.name
        try:
            ctx.load_verify_locations(cafile=ca_path)
        finally:
            try:
                os.remove(ca_path)
            except OSError:
                pass

    return ctx
