from random import Random


def create_rng(seed: int) -> Random:
    """Create an isolated pseudo-random generator for a simulation run."""

    if seed < 0:
        raise ValueError("seed must be non-negative")
    return Random(seed)
