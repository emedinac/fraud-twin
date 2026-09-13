import hashlib
from random import Random


def create_rng(seed: int) -> Random:
    """Create an isolated pseudo-random generator for a simulation run."""

    if seed < 0:
        raise ValueError("seed must be non-negative")
    return Random(seed)


def create_stream_rng(seed: int, stream: str) -> Random:
    """Create a deterministic RNG isolated from every other named stream."""

    if seed < 0:
        raise ValueError("seed must be non-negative")
    return _rng_from_material(f"fraudtwin:{stream}:{seed}")


def create_legacy_entity_stream_rng(seed: int, entity_type: str) -> Random:
    """Create the original M1 entity stream without changing its seed material."""

    if seed < 0:
        raise ValueError("seed must be non-negative")
    return _rng_from_material(f"fraudtwin:milestone-1:{seed}:{entity_type}")


def _rng_from_material(material: str) -> Random:
    stream_seed = int.from_bytes(hashlib.sha256(material.encode()).digest()[:8], "big")
    return create_rng(stream_seed)
