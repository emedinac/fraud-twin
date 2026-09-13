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
    material = f"fraudtwin:{stream}:{seed}".encode()
    stream_seed = int.from_bytes(hashlib.sha256(material).digest()[:8], "big")
    return create_rng(stream_seed)
