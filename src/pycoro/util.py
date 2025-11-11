from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class KV[K: str, V]:
    key: K
    value: V


def ordered_range_sort[K: str, V](m: dict[K, V]) -> list[K]:
    keys = list(m.keys())
    keys.sort()
    for i in range(len(keys) - 1):
        assert keys[i] <= keys[i + 1], "slice not sorted"
    return keys


def ordered_range[K: str, V](m: dict[K, V]) -> list[V]:
    return [m[key] for key in ordered_range_sort(m)]


def ordered_range_kv[K: str, V](m: dict[K, V]) -> list[KV[K, V]]:
    return [KV(key, m[key]) for key in ordered_range_sort(m)]
