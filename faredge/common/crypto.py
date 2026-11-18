"""Cryptographic primitives for the Distributed Ledger Enabler.

The paper requires that ledger state be "secured by cryptographic techniques,
so that records in the ledger cannot be modified or removed once created"
(S3.3).  Two primitives are needed for that:

1.  A collision-resistant hash chain + Merkle tree, so that any modification
    of a committed record invalidates every subsequent block.
2.  Digital signatures, so that the author of a transaction and the proposer
    of a block are verifiable by every Peer Node.

To keep the project dependency-free we implement textbook RSA on top of the
standard library instead of pulling in `cryptography`/`ecdsa`.  Keys default
to a 512-bit modulus: large enough that signatures are genuinely asymmetric
and verifiable, small enough that spinning up dozens of Peer Nodes and Smart
Objects in a unit test is instantaneous.

    WARNING - simulation grade only.  Raw (unpadded) RSA over a SHA-256
    digest with a small modulus is adequate for a research prototype but must
    be replaced by a vetted library (e.g. ECDSA P-256) before any real
    deployment.
"""
from __future__ import annotations

import hashlib
import json
import secrets
from dataclasses import dataclass
from typing import Any, List, Sequence, Tuple

_PUBLIC_EXPONENT = 65537


# --------------------------------------------------------------------------
# Hashing
# --------------------------------------------------------------------------
def sha256_hex(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def canonical_json(obj: Any) -> str:
    """Deterministic serialisation.

    Smart contracts must be deterministic so that every Peer Node, executing
    the same call in parallel, derives bit-identical state (paper S2.3:
    "each service call is executed in parallel by all Peer Nodes ... which
    then need to reach a consensus on its validity").  Deterministic
    serialisation is what makes the resulting state root comparable.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def hash_object(obj: Any) -> str:
    return sha256_hex(canonical_json(obj))


def merkle_root(leaves: Sequence[str]) -> str:
    """Merkle root over the transaction hashes of a block."""
    if not leaves:
        return sha256_hex(b"")
    level: List[str] = list(leaves)
    while len(level) > 1:
        if len(level) % 2:
            level.append(level[-1])
        level = [sha256_hex(level[i] + level[i + 1]) for i in range(0, len(level), 2)]
    return level[0]


# --------------------------------------------------------------------------
# RSA key material
# --------------------------------------------------------------------------
def _is_probable_prime(n: int, rounds: int = 24) -> bool:
    if n < 2:
        return False
    for p in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37):
        if n % p == 0:
            return n == p
    d, r = n - 1, 0
    while d % 2 == 0:
        d //= 2
        r += 1
    for _ in range(rounds):
        a = secrets.randbelow(n - 3) + 2
        x = pow(a, d, n)
        if x in (1, n - 1):
            continue
        for _ in range(r - 1):
            x = x * x % n
            if x == n - 1:
                break
        else:
            return False
    return True


def _random_prime(bits: int) -> int:
    while True:
        candidate = secrets.randbits(bits) | (1 << (bits - 1)) | 1
        if _is_probable_prime(candidate):
            return candidate


@dataclass(frozen=True)
class PublicKey:
    n: int
    e: int = _PUBLIC_EXPONENT

    @property
    def fingerprint(self) -> str:
        """Short, stable identity derived from the key - the node's address."""
        return sha256_hex(f"{self.n}:{self.e}")[:16]

    def to_dict(self) -> dict:
        return {"n": str(self.n), "e": self.e}


@dataclass(frozen=True)
class PrivateKey:
    n: int
    d: int


class Identity:
    """A key pair plus the derived address, as held by a Peer Node, an Edge
    Gateway or a Smart Object."""

    def __init__(self, name: str, bits: int = 512):
        half = max(128, bits // 2)
        while True:
            p, q = _random_prime(half), _random_prime(half)
            if p == q:
                continue
            n = p * q
            phi = (p - 1) * (q - 1)
            if phi % _PUBLIC_EXPONENT == 0:
                continue
            d = pow(_PUBLIC_EXPONENT, -1, phi)
            self.name = name
            self.public = PublicKey(n, _PUBLIC_EXPONENT)
            self._private = PrivateKey(n, d)
            return

    @property
    def address(self) -> str:
        return self.public.fingerprint

    def sign(self, payload: Any) -> str:
        digest = int(hash_object(payload), 16) % self._private.n
        return hex(pow(digest, self._private.d, self._private.n))

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<Identity {self.name} addr={self.address}>"


def verify(public: PublicKey, payload: Any, signature: str) -> bool:
    try:
        sig = int(signature, 16)
    except (TypeError, ValueError):
        return False
    expected = int(hash_object(payload), 16) % public.n
    return pow(sig, public.e, public.n) == expected


class KeyDirectory:
    """Maps addresses to public keys.

    Stands in for the Identity Management (IDM) Component of the Cloud Tier
    (paper S3.4) as far as the ledger is concerned: a Peer Node validating a
    transaction must be able to look up the author's public key.
    """

    def __init__(self) -> None:
        self._keys: dict[str, PublicKey] = {}

    def register(self, identity: Identity) -> str:
        self._keys[identity.address] = identity.public
        return identity.address

    def get(self, address: str) -> PublicKey | None:
        return self._keys.get(address)

    def verify_signed(self, address: str, payload: Any, signature: str) -> bool:
        key = self.get(address)
        return bool(key) and verify(key, payload, signature)

    def __len__(self) -> int:
        return len(self._keys)
