"""
MeshWeaver Security Package
Provides TLS/SSL transport wrapping and cryptographic message signing.
"""

from .crypto import (
    ReplayAttackError,
    SecurityError,
    SecurityManager,
    SignatureVerificationError,
    sign_task_request,
    sign_task_result,
    verify_task_request,
    verify_task_result,
)
from .tls import (
    TLSConfigurationError,
    TLSMemoryChannel,
    create_client_ssl_context,
    create_server_ssl_context,
    ensure_node_certificates,
    generate_self_signed_cert,
)

__all__ = [
    "SecurityError",
    "SignatureVerificationError",
    "ReplayAttackError",
    "SecurityManager",
    "sign_task_request",
    "verify_task_request",
    "sign_task_result",
    "verify_task_result",
    "TLSConfigurationError",
    "TLSMemoryChannel",
    "generate_self_signed_cert",
    "ensure_node_certificates",
    "create_server_ssl_context",
    "create_client_ssl_context",
]
