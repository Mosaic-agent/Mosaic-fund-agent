"""
Edelweiss MF API — authentication helpers and AES-256-CBC decryption.

This module centralises the crypto constants, header definitions, and the
Node.js-backed decryption routine so that every Edelweiss importer can
reuse them without duplicating sensitive logic.

Decryption flow
---------------
1. Compute HMAC-SHA256(HASH_KEY, SECRET + STATIC_IP + timestamp) → hex string.
2. Use that hex string as the *password* for OpenSSL-style EVP key derivation
   (MD5-based) to produce a 32-byte key and 16-byte IV.
3. The ciphertext is base64-decoded; the first 16 bytes are ``Salted__`` (magic)
   followed by an 8-byte salt used in the EVP derivation.
4. AES-256-CBC decrypt the remaining bytes.
"""

from __future__ import annotations

import json
import logging
import subprocess
import textwrap
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

API_BASE: str = "https://api.edelweissmf.com/edelweissmf/api/v1"
"""Root URL for the Edelweiss MF public API (v1)."""

STATIC_IP: str = "103.0.123.175"
"""IP address sent in the ``x-ip-address`` header for every API call."""

SECRET: str = "5b6714126d3149fbab994747b2633287"
"""Shared secret used in the HMAC-SHA256 password derivation."""

HASH_KEY: str = "r4vcos0ejvndsow95n"
"""HMAC key used together with *SECRET*, *STATIC_IP*, and the timestamp."""

HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.edelweissmf.com/",
}
"""Default HTTP headers attached to every Edelweiss API request."""

# ---------------------------------------------------------------------------
# Node.js decrypt script (template)
# ---------------------------------------------------------------------------
# Secrets are baked in as constants — they are *not* user-supplied.
# The only dynamic values (ciphertext & timestamp) arrive via **stdin** as
# JSON so we never interpolate untrusted data into a shell command.
# ---------------------------------------------------------------------------

_NODE_DECRYPT_SCRIPT: str = textwrap.dedent(
    f"""\
    const crypto = require('crypto');
    const fs     = require('fs');

    // ── constants (not user input) ──────────────────────────────────────
    const SECRET   = '{SECRET}';
    const HASH_KEY = '{HASH_KEY}';
    const IP       = '{STATIC_IP}';

    // ── dynamic values from stdin ───────────────────────────────────────
    const input = JSON.parse(fs.readFileSync('/dev/stdin', 'utf8'));
    const {{ ciphertext, timestamp }} = input;

    // ── HMAC-SHA256 password derivation ─────────────────────────────────
    const message  = SECRET + IP + timestamp;
    const password = crypto
        .createHmac('sha256', HASH_KEY)
        .update(message)
        .digest('hex');

    // ── OpenSSL EVP-style key derivation (MD5) ──────────────────────────
    const encryptedBytes = Buffer.from(ciphertext, 'base64');
    // First 8 bytes: "Salted__", next 8 bytes: salt
    const salt       = encryptedBytes.slice(8, 16);
    const cipherData = encryptedBytes.slice(16);

    function evpKDF(password, salt, keyLen, ivLen) {{
        const passBuf  = Buffer.from(password, 'utf8');
        const totalLen = keyLen + ivLen;
        let derived    = Buffer.alloc(0);
        let block      = Buffer.alloc(0);

        while (derived.length < totalLen) {{
            const hash = crypto.createHash('md5');
            hash.update(Buffer.concat([block, passBuf, salt]));
            block   = hash.digest();
            derived = Buffer.concat([derived, block]);
        }}

        return {{
            key: derived.slice(0, keyLen),
            iv:  derived.slice(keyLen, keyLen + ivLen),
        }};
    }}

    const {{ key, iv }} = evpKDF(password, salt, 32, 16);

    // ── AES-256-CBC decrypt ─────────────────────────────────────────────
    const decipher  = crypto.createDecipheriv('aes-256-cbc', key, iv);
    let decrypted   = decipher.update(cipherData, undefined, 'utf8');
    decrypted      += decipher.final('utf8');

    process.stdout.write(decrypted);
    """
)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def decrypt_payload(ciphertext_b64: str, timestamp_str: str) -> dict[str, Any]:
    """Decrypt an Edelweiss API response payload.

    The ciphertext and timestamp are passed to a short-lived Node.js process
    via **stdin** (as JSON) to avoid any shell-injection risk.

    Parameters
    ----------
    ciphertext_b64:
        Base64-encoded ciphertext returned by the API (``response.text``).
    timestamp_str:
        The timestamp string that was sent in the ``x-timestamp`` header of
        the original request.

    Returns
    -------
    dict
        The decrypted and JSON-parsed response body.

    Raises
    ------
    subprocess.CalledProcessError
        If the Node.js process exits with a non-zero code.
    json.JSONDecodeError
        If the decrypted output is not valid JSON.
    """
    stdin_payload = json.dumps(
        {"ciphertext": ciphertext_b64, "timestamp": timestamp_str}
    )

    logger.debug(
        "Decrypting payload (ciphertext length=%d, timestamp=%s)",
        len(ciphertext_b64),
        timestamp_str,
    )

    result = subprocess.run(
        ["node", "-e", _NODE_DECRYPT_SCRIPT],
        input=stdin_payload,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )

    if result.stderr:
        logger.warning("Node.js stderr output: %s", result.stderr.strip())

    decrypted: dict[str, Any] = json.loads(result.stdout)
    logger.debug("Decryption succeeded — top-level keys: %s", list(decrypted.keys()))
    return decrypted


def get_authenticated_data(
    endpoint: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fetch and decrypt data from the Edelweiss MF API.

    Builds the full URL from :pydata:`API_BASE` ``+ "/" +`` *endpoint*,
    attaches the required ``x-timestamp`` and ``x-ip-address`` headers,
    performs a ``GET`` request, and returns the decrypted JSON payload.

    Parameters
    ----------
    endpoint:
        Path segment appended to :pydata:`API_BASE`
        (e.g. ``"SchemeNAV"`` or ``"FundDetails"``).
    params:
        Optional query-string parameters forwarded to :func:`requests.get`.

    Returns
    -------
    dict
        Decrypted JSON response from the API.

    Raises
    ------
    requests.HTTPError
        If the API returns a non-2xx status code.
    """
    timestamp_str = str(int(datetime.now(tz=timezone.utc).timestamp() * 1000))
    url = f"{API_BASE}/{endpoint}"

    request_headers = {
        **HEADERS,
        "x-timestamp": timestamp_str,
        "x-ip-address": STATIC_IP,
    }

    logger.debug("GET %s  (timestamp=%s)", url, timestamp_str)


    response = requests.get(
        url,
        headers=request_headers,
        params=params,
        timeout=60,
    )
    response.raise_for_status()

    logger.debug(
        "Response status=%d, content-length=%s",
        response.status_code,
        response.headers.get("Content-Length", "unknown"),
    )

    try:
        data = response.json()
        ciphertext = data.get("body", "") if isinstance(data, dict) else response.text
    except Exception:
        ciphertext = response.text

    if not ciphertext:
        return {}

    return decrypt_payload(
        ciphertext_b64=ciphertext,
        timestamp_str=timestamp_str,
    )

