"""Tiny indirection so that network.Link can see the active NetworkConfig
without importing the whole platform (avoids an import cycle)."""
from __future__ import annotations

from ..config import NetworkConfig

_ACTIVE = NetworkConfig()


def set_network_config(cfg: NetworkConfig) -> None:
    global _ACTIVE
    _ACTIVE = cfg


def get_network_config() -> NetworkConfig:
    return _ACTIVE
