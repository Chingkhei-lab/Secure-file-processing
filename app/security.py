"""Pure cryptographic helpers. No I/O here, so they are easy to unit test.

Design
------
* An API key is a long random string. We store only its SHA-256 hash.
  (A fast hash is fine: the key is 256 bits of randomness, so there is
  nothing to brute-force, unlike a human-chosen password.)
* Every request is signed with HMAC-SHA256 using a per-key *signing secret*.
  The server does not store that secret: it re-derives it as
      HMAC(SIGNING_MASTER_SECRET, api_key_id)
  The client receives it once, when the key is created.
* The signature covers the timestamp, HTTP method, path+query and the SHA-256
  of the body, so none of those can be altered without invalidating it.
"""
import hashlib
import hmac
import re
import secrets
import uuid

KEY_PREFIX = "fsk_"
KEY_PREFIX_LEN = 12  # "fsk_" + 8 chars, stored for humans to identify keys
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def generate_api_key() -> str:
    return KEY_PREFIX + secrets.token_urlsafe(32)


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()


def derive_signing_secret(master_secret: str, api_key_id: uuid.UUID) -> str:
    return hmac.new(
        master_secret.encode(), str(api_key_id).encode(), hashlib.sha256
    ).hexdigest()


def is_valid_sha256_hex(value: str) -> bool:
    return bool(_SHA256_RE.match(value))


def canonical_string(timestamp: str, method: str, target: str, content_sha256: str) -> str:
    """The exact text that gets signed. `target` is path plus '?query' if any."""
    return "\n".join([timestamp, method.upper(), target, content_sha256])


def compute_signature(
    signing_secret: str, timestamp: str, method: str, target: str, content_sha256: str
) -> str:
    message = canonical_string(timestamp, method, target, content_sha256)
    return hmac.new(signing_secret.encode(), message.encode(), hashlib.sha256).hexdigest()
