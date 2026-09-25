"""Small, dependency-free helpers for generation stage measurements."""

import resource
import time
from collections.abc import MutableMapping
from contextlib import AbstractContextManager
from types import TracebackType

StageMetrics = dict[str, dict[str, float]]


def peak_rss_mb() -> float:
    """Return the process peak resident set size in MiB when available."""

    # Linux and macOS report ru_maxrss in different units.
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value / (1024.0 * 1024.0) if value > 1024.0 * 1024.0 else value / 1024.0


def finish_stage(
    timings: MutableMapping[str, dict[str, float]] | None,
    name: str,
    started: float,
) -> None:
    """Store measurements for a stage started with :func:`time.perf_counter`."""

    if timings is not None:
        timings[name] = {
            "elapsed_seconds": time.perf_counter() - started,
            "peak_rss_mb": peak_rss_mb(),
        }


class _StageMeasurement(AbstractContextManager[None]):
    """Record elapsed time and peak RSS for a named stage when requested."""

    def __init__(self, timings: MutableMapping[str, dict[str, float]] | None, name: str) -> None:
        self._timings = timings
        self._name = name
        self._started: float | None = None

    def __enter__(self) -> None:
        self._started = time.perf_counter()
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._started is not None:
            finish_stage(self._timings, self._name, self._started)


def measure_stage(
    timings: MutableMapping[str, dict[str, float]] | None, name: str
) -> AbstractContextManager[None]:
    """Return a context manager recording elapsed time and peak RSS."""

    return _StageMeasurement(timings, name)
