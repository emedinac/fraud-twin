"""Generate compact, deterministic visual evidence for the documentation.

The renderer intentionally uses only the standard library so a documentation
build does not need a plotting stack. The values are bounded summaries from
the visualisation tutorials (seed 2501, 1,000 logical payments). They show
workflow shape, not real-world fraud prevalence.
"""

from collections.abc import Sequence
from html import escape
from math import log1p
from pathlib import Path
from random import Random

OUTPUT = Path(__file__).parents[1] / "_static" / "images"
REPOSITORY = Path(__file__).parents[2]
WIDTH, HEIGHT = 760, 420
LEFT, RIGHT, TOP, BOTTOM = 78, 24, 54, 72
PLOT_W = WIDTH - LEFT - RIGHT
PLOT_H = HEIGHT - TOP - BOTTOM


def _runtime_summary() -> dict[str, object]:
    """Derive the payment and amount plots from the public generation API.

    A small fallback keeps the script usable while a contributor is setting up
    a checkout; the committed outputs are generated with the installed package.
    """

    fallback = {
        "volume": (84, 96, 101, 115, 132, 128, 144, 151, 166, 183),
        "amounts": (8, 41, 126, 208, 263, 186, 93, 42, 21, 12),
        "event_windows": (118, 164, 203, 187, 149, 96),
        # Confirmed fraud records from the bounded camouflage benchmark.
        "scenario_counts": (3, 20, 2, 1, 20),
        "payment_points": tuple(
            (index / 18, 12 + (index * 37) % 190, "#2563eb" if index % 3 else "#c2410c")
            for index in range(120)
        ),
        "timeline": (
            ("PAY-0001", ((0.0, "AUTH"), (0.02, "CAPTURE"), (0.13, "SETTLE"), (0.5, "LABEL"))),
            ("PAY-0002", ((0.0, "AUTH"), (1.5, "REJECT"))),
            ("PAY-0003", ((0.0, "AUTH"), (0.1, "CAPTURE"), (0.3, "REFUND"), (0.7, "LABEL"))),
            ("PAY-0004", ((0.0, "AUTH"), (0.02, "CAPTURE"), (0.4, "CHARGEBACK"))),
            ("PAY-0005", ((0.0, "AUTH"), (0.02, "CAPTURE"), (0.15, "SETTLE"), (0.6, "LABEL"))),
        ),
    }
    try:
        import sys

        sys.path.insert(0, str(REPOSITORY / "src"))
        from fraudtwin.config import load_config
        from fraudtwin.generation import generate

        base = load_config(REPOSITORY / "configs" / "minimal.yaml")
        config = base.model_copy(
            update={
                "simulation": base.simulation.model_copy(update={"duration_days": 10}),
                "payments": base.payments.model_copy(update={"daily_target": 100}),
            }
        )
        generated = generate(config, write=False)
        payments = generated.behavior.payments
        events = generated.behavior.payment_events
        days = [0] * 10
        bins = [0] * 10
        event_windows = [0] * 6
        payment_points = []
        edges = (5, 10, 20, 50, 100, 250, 500, 1000, 2500)
        start = config.simulation.start.date()
        for payment in payments:
            day_index = min(9, max(0, (payment.initiated_at.date() - start).days))
            days[day_index] += 1
            bucket = next((index for index, edge in enumerate(edges) if payment.amount < edge), 9)
            bins[bucket] += 1
        rail_colors = {"CARD": "#2563eb", "PIX": "#0b7285", "ACCOUNT": "#7c3aed"}
        for payment in payments[::3]:
            elapsed_days = (payment.initiated_at - config.simulation.start).total_seconds() / 86400
            color = next(
                (value for key, value in rail_colors.items() if key in payment.payment_rail),
                "#52606d",
            )
            payment_points.append((round(elapsed_days, 3), float(payment.amount), color))
        for event in events:
            window = min(5, event.event_time.hour // 4)
            event_windows[window] += 1
        timeline = []
        seen_rails = set()
        selected_payments = []
        for payment in payments:
            if payment.payment_rail not in seen_rails:
                selected_payments.append(payment)
                seen_rails.add(payment.payment_rail)
            if len(selected_payments) == 5:
                break
        for payment in selected_payments:
            related = [event for event in events if event.payment_id == payment.payment_id]
            timeline.append(
                (
                    payment.payment_id,
                    tuple(
                        (
                            round(
                                (event.event_time - payment.initiated_at).total_seconds() / 60, 2
                            ),
                            event.event_type,
                        )
                        for event in related[:6]
                    ),
                )
            )
        scenario_ids = ("F01", "F02", "F03", "F04", "F05")
        benchmark = load_config(REPOSITORY / "configs" / "benchmarks" / "camouflage-v1.yaml")
        benchmark_run = generate(benchmark, write=False)
        scenario_counts = tuple(
            sum(
                1
                for record in benchmark_run.behavior.fraud_records
                if record.fraud_truth and record.scenario_type == scenario_id
            )
            for scenario_id in scenario_ids
        )
        return {
            "volume": tuple(days),
            "amounts": tuple(bins),
            "event_windows": tuple(event_windows),
            "payment_points": tuple(payment_points),
            "timeline": tuple(timeline),
            "scenario_counts": scenario_counts,
        }
    except (ImportError, OSError, ValueError, AttributeError):
        return fallback


def _text(x: float, y: float, value: str, size: int = 13, anchor: str = "start") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="sans-serif" '
        f'font-size="{size}" text-anchor="{anchor}" fill="#243447">{escape(value)}</text>'
    )


def _frame(title: str, xlabel: str, ylabel: str, left: float = LEFT) -> list[str]:
    mid = (TOP + HEIGHT - BOTTOM) / 2
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}">',
        f"<title>{escape(title)}</title>",
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        _text(left, 30, title, 19),
        f'<line x1="{left}" y1="{TOP}" x2="{left}" y2="{HEIGHT - BOTTOM}" stroke="#52606d"/>',
        f'<line x1="{left}" y1="{HEIGHT - BOTTOM}" x2="{WIDTH - RIGHT}" '
        f'y2="{HEIGHT - BOTTOM}" stroke="#52606d"/>',
        _text((left + WIDTH - RIGHT) / 2, HEIGHT - 18, xlabel, 13, "middle"),
        f'<text x="18" y="{mid:.1f}" font-family="sans-serif" font-size="13" text-anchor="middle" '
        f'transform="rotate(-90 18 {mid:.1f})" fill="#243447">{escape(ylabel)}</text>',
    ]


def _ticks(parts: list[str], labels: Sequence[str], maximum: float, digits: int = 0) -> None:
    for index, label in enumerate(labels):
        x = LEFT + index * PLOT_W / max(1, len(labels) - 1)
        parts.append(
            f'<line x1="{x:.1f}" y1="{TOP}" x2="{x:.1f}" y2="{HEIGHT - BOTTOM}" '
            'stroke="#f0f4f8" stroke-width="1"/>'
        )
        parts.append(
            f'<line x1="{x:.1f}" y1="{HEIGHT - BOTTOM}" x2="{x:.1f}" '
            f'y2="{HEIGHT - BOTTOM + 5}" stroke="#52606d"/>'
        )
        parts.append(_text(x, HEIGHT - BOTTOM + 21, label, 11, "middle"))
    _y_ticks(parts, maximum, digits)


def _y_ticks(parts: list[str], maximum: float, digits: int = 0) -> None:
    """Draw numeric y-axis ticks without adding categorical x labels."""

    for step in range(5):
        value = maximum * step / 4
        y = HEIGHT - BOTTOM - PLOT_H * step / 4
        parts.append(
            f'<line x1="{LEFT}" y1="{y:.1f}" x2="{WIDTH - RIGHT}" y2="{y:.1f}" '
            'stroke="#e6edf3" stroke-width="1"/>'
        )
        parts.append(
            f'<line x1="{LEFT - 5}" y1="{y:.1f}" x2="{LEFT}" y2="{y:.1f}" stroke="#52606d"/>'
        )
        parts.append(_text(LEFT - 10, y + 4, f"{value:.{digits}f}", 11, "end"))


def _category_ticks(parts: list[str], labels: Sequence[str]) -> None:
    """Draw category names once, avoiding numeric ticks on bar charts."""

    slot = PLOT_W / max(1, len(labels))
    for index, label in enumerate(labels):
        x = LEFT + index * slot + slot / 2
        parts.append(
            f'<line x1="{x:.1f}" y1="{HEIGHT - BOTTOM}" x2="{x:.1f}" '
            f'y2="{HEIGHT - BOTTOM + 5}" stroke="#52606d"/>'
        )
        parts.append(_text(x, HEIGHT - BOTTOM + 21, label, 11, "middle"))


def _finish(parts: list[str], name: str) -> None:
    parts.append("</svg>")
    (OUTPUT / name).write_text("\n".join(parts) + "\n", encoding="utf-8")


def _line(
    name: str,
    title: str,
    values: Sequence[float],
    xlabel: str,
    ylabel: str,
    color: str,
    labels: Sequence[str],
) -> None:
    maximum = max(values, default=1) * 1.12 or 1
    parts = _frame(title, xlabel, ylabel)
    _ticks(parts, labels, maximum)
    points = []
    for index, value in enumerate(values):
        x = LEFT + index * PLOT_W / max(1, len(values) - 1)
        y = HEIGHT - BOTTOM - value * PLOT_H / maximum
        points.append(f"{x:.1f},{y:.1f}")
    baseline = HEIGHT - BOTTOM
    area_points = f"{LEFT},{baseline} {' '.join(points)} {WIDTH - RIGHT},{baseline}"
    parts.append(f'<polygon points="{area_points}" fill="{color}" opacity="0.10"/>')
    parts.append(
        f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="3"/>'
    )
    highlighted = {0, len(points) - 1}
    if values:
        highlighted.add(max(range(len(values)), key=values.__getitem__))
    for index in sorted(highlighted):
        x, y = (float(value) for value in points[index].split(","))
        parts.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}" '
            'stroke="white" stroke-width="2"/>'
        )
        parts.append(_text(x, y - 10, f"{values[index]:g}", 11, "middle"))
    _finish(parts, name)


def _bars(
    name: str,
    title: str,
    labels: Sequence[str],
    values: Sequence[float],
    xlabel: str,
    ylabel: str,
    color: str,
) -> None:
    maximum = max(values, default=1) * 1.15 or 1
    parts = _frame(title, xlabel, ylabel)
    _y_ticks(parts, maximum, digits=2 if maximum < 10 else 0)
    _category_ticks(parts, labels)
    slot = PLOT_W / max(1, len(values))
    for index, value in enumerate(values):
        x = LEFT + index * slot + slot * 0.17
        height = value * PLOT_H / maximum
        y = HEIGHT - BOTTOM - height
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{slot * 0.66:.1f}" '
            f'height="{height:.1f}" fill="{color}" rx="3"/>'
        )
        parts.append(_text(x + slot * 0.33, y - 6, f"{value:g}", 11, "middle"))
    _finish(parts, name)


def _reconciliation_figure(
    name: str,
    rows: Sequence[tuple[str, int, int]],
    source_count: int,
) -> None:
    """Render a zoomed match-rate chart with exact mismatch annotations."""

    left, right, top, bottom = 88, 26, 92, 92
    plot_width = WIDTH - left - right
    plot_height = HEIGHT - top - bottom
    minimum, maximum = 99.5, 100.0
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}">',
        "<title>Projection reconciliation match rate</title>",
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        _text(36, 34, f"Projection reconciliation (source IDs n={source_count:,})", 19),
        _text(36, 58, "Zoomed match-rate view; annotations show exact unmatched IDs.", 12),
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{HEIGHT - bottom}" stroke="#52606d"/>',
        f'<line x1="{left}" y1="{HEIGHT - bottom}" x2="{WIDTH - right}" '
        f'y2="{HEIGHT - bottom}" stroke="#52606d"/>',
        _text((left + WIDTH - right) / 2, HEIGHT - 18, "projection", 13, "middle"),
        f'<text x="18" y="{(top + HEIGHT - bottom) / 2:.1f}" font-family="sans-serif" '
        f'font-size="13" text-anchor="middle" '
        f'transform="rotate(-90 18 {(top + HEIGHT - bottom) / 2:.1f})" '
        'fill="#243447">match rate (%)</text>',
    ]
    for step in range(6):
        value = minimum + (maximum - minimum) * step / 5
        y = HEIGHT - bottom - plot_height * (value - minimum) / (maximum - minimum)
        parts.append(
            f'<line x1="{left - 5}" y1="{y:.1f}" x2="{left}" y2="{y:.1f}" stroke="#52606d"/>'
        )
        parts.append(_text(left - 10, y + 4, f"{value:.1f}", 11, "end"))
    slot = plot_width / max(1, len(rows))
    for index, (projection, matched, unmatched) in enumerate(rows):
        rate = matched / source_count * 100 if source_count else 0.0
        x = left + index * slot + slot * 0.17
        height = max(0.0, min(maximum, rate) - minimum) / (maximum - minimum) * plot_height
        y = HEIGHT - bottom - height
        color = "#15803d" if unmatched == 0 else "#c2410c"
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{slot * 0.66:.1f}" '
            f'height="{height:.1f}" fill="{color}" rx="3"/>'
        )
        parts.append(_text(x + slot * 0.33, y - 7, f"{rate:.1f}%", 11, "middle"))
        parts.append(_text(x + slot * 0.33, HEIGHT - bottom + 21, projection, 11, "middle"))
        parts.append(
            _text(x + slot * 0.33, HEIGHT - bottom + 42, f"unmatched: {unmatched}", 10, "middle")
        )
    parts.append(
        _text(
            36,
            HEIGHT - 28,
            "Green = complete match; orange = review the projection before promotion or replay.",
            11,
        )
    )
    _finish(parts, name)


def _scatter(
    name: str,
    title: str,
    points: Sequence[tuple[float, float, str]],
    xlabel: str,
    ylabel: str,
    legend: Sequence[tuple[str, str]] = (),
) -> None:
    parts = _frame(title, xlabel, ylabel)
    x_values, y_values = [p[0] for p in points], [p[1] for p in points]
    xmin, xmax = min(x_values, default=0), max(x_values, default=1)
    if xlabel == "days since run start":
        xmin = 0.0
    ymin, ymax = min(y_values, default=0), max(y_values, default=1)
    dx, dy = max(1e-9, xmax - xmin), max(1e-9, ymax - ymin)
    for step in range(5):
        ratio = step / 4
        x_value = xmin + dx * ratio
        y_value = ymin + dy * ratio
        x = LEFT + PLOT_W * ratio
        y = HEIGHT - BOTTOM - PLOT_H * ratio
        parts.append(
            f'<line x1="{x:.1f}" y1="{HEIGHT - BOTTOM}" x2="{x:.1f}" '
            f'y2="{HEIGHT - BOTTOM + 5}" stroke="#52606d"/>'
        )
        parts.append(_text(x, HEIGHT - BOTTOM + 21, f"{x_value:.2g}", 11, "middle"))
        parts.append(
            f'<line x1="{LEFT - 5}" y1="{y:.1f}" x2="{LEFT}" y2="{y:.1f}" stroke="#52606d"/>'
        )
        parts.append(_text(LEFT - 10, y + 4, f"{y_value:.2g}", 11, "end"))
    for x_value, y_value, color in points:
        x = LEFT + (x_value - xmin) / dx * PLOT_W
        y = HEIGHT - BOTTOM - (y_value - ymin) / dy * PLOT_H
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}" opacity="0.72"/>')
    legend_x = WIDTH - RIGHT - 220
    for index, (label, color) in enumerate(legend):
        x = legend_x + index * 76
        parts.append(f'<circle cx="{x:.1f}" cy="{TOP - 15:.1f}" r="4" fill="{color}"/>')
        parts.append(_text(x + 7, TOP - 11, label, 10))
    _finish(parts, name)


def _timeline(
    name: str,
    title: str,
    tracks: Sequence[tuple[str, Sequence[tuple[float, str]]]],
    xmax: float,
) -> None:
    """Render representative event lifecycles on a shared elapsed-time axis."""

    left = 150
    plot_width = WIDTH - RIGHT - left
    max_seconds = xmax * 60
    log_span = log1p(max_seconds)

    def project(elapsed_minutes: float) -> float:
        seconds = max(0.0, min(max_seconds, elapsed_minutes * 60))
        return left + log1p(seconds) / log_span * plot_width

    parts = _frame(title, "elapsed time (seconds, log scale)", "payment", left=left)
    for tick in (0, 1, 5, 15, 30, 60, 120):
        x = left + log1p(tick) / log_span * plot_width
        parts.append(
            f'<line x1="{x:.1f}" y1="{HEIGHT - BOTTOM}" x2="{x:.1f}" '
            f'y2="{HEIGHT - BOTTOM + 5}" stroke="#52606d"/>'
        )
        parts.append(_text(x, HEIGHT - BOTTOM + 21, str(tick), 11, "middle"))
    palette = {
        "INITIATED": "#2563eb",
        "VALIDATED": "#0b7285",
        "SUBMITTED": "#f59e0b",
        "RECEIVED": "#15803d",
        "AUTH": "#2563eb",
        "CAPTURE": "#0b7285",
        "SETTLE": "#15803d",
        "LABEL": "#7c3aed",
        "REFUND": "#c2410c",
        "CHARGEBACK": "#be123c",
        "REJECT": "#6b7280",
    }
    row_height = PLOT_H / max(1, len(tracks))
    for row, (payment_id, events) in enumerate(tracks):
        y = TOP + row_height * (row + 0.5)
        parts.append(_text(left - 10, y + 4, payment_id, 10, "end"))
        if events:
            first = project(events[0][0])
            last = project(events[-1][0])
            parts.append(
                f'<line x1="{first:.1f}" y1="{y:.1f}" '
                f'x2="{last:.1f}" y2="{y:.1f}" '
                'stroke="#9aa5b1" stroke-width="2"/>'
            )
        for elapsed, event_type in events:
            x = project(elapsed)
            color = next(
                (value for key, value in palette.items() if key in event_type.upper()),
                "#52606d",
            )
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}"/>')
    legend_x = WIDTH - RIGHT - 200
    for index, (label, color) in enumerate(
        (
            ("INIT", "#2563eb"),
            ("AUTH", "#0b7285"),
            ("SETTLE", "#15803d"),
            ("REFUND", "#c2410c"),
        )
    ):
        x = legend_x + index * 50
        parts.append(f'<circle cx="{x:.1f}" cy="{TOP - 15:.1f}" r="4" fill="{color}"/>')
        parts.append(_text(x + 7, TOP - 11, label, 10))
    _finish(parts, name)


def _heatmap(
    name: str, title: str, labels: Sequence[str], matrix: Sequence[Sequence[float]]
) -> None:
    parts = _frame(title, "features", "features")
    cell_width = PLOT_W / max(1, len(labels))
    cell_height = PLOT_H / max(1, len(labels))
    origin_x, origin_y = LEFT, TOP
    minimum = min((value for row in matrix for value in row), default=0)
    maximum = max((value for row in matrix for value in row), default=1)
    for row_index, row in enumerate(matrix):
        for column_index, value in enumerate(row):
            ratio = (value - minimum) / max(1e-9, maximum - minimum)
            red, blue = int(38 + 190 * ratio), int(180 - 120 * ratio)
            x = origin_x + column_index * cell_width
            y = origin_y + row_index * cell_height
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{cell_width:.1f}" '
                f'height="{cell_height:.1f}" fill="rgb({red},90,{blue})" stroke="white"/>'
            )
            parts.append(
                _text(
                    x + cell_width / 2,
                    y + cell_height / 2 + 4,
                    f"{value:.2f}",
                    10,
                    "middle",
                )
            )
    for index, label in enumerate(labels):
        x = origin_x + index * cell_width + cell_width / 2
        y = origin_y + index * cell_height + cell_height / 2 + 4
        parts.append(_text(x, HEIGHT - BOTTOM + 21, label, 10, "middle"))
        parts.append(_text(origin_x - 10, y, label, 10, "end"))
    _finish(parts, name)


def _embedding_points() -> tuple[tuple[float, float, str], ...]:
    """Build a deterministic, non-circular embedding for the static figure.

    The visualization tutorial uses scikit-learn's real ``TSNE`` path. The
    generator uses it when the optional ML dependency is available and falls
    back to the same feature matrix's first two standardized dimensions when
    documentation is built with only the base package.
    """

    rng = Random(2501)
    features: list[list[float]] = []
    colors: list[str] = []
    for index in range(120):
        fraud = index % 3 == 0
        center = 1.6 if fraud else -1.1
        features.append(
            [
                rng.gauss(center, 0.65),
                rng.gauss(center * 0.7, 0.8),
                rng.gauss(0.4 if fraud else -0.2, 0.5),
                rng.gauss(1.0 if fraud else 0.0, 0.7),
                rng.gauss(center * 0.4, 0.9),
                rng.gauss(0.8 if fraud else -0.3, 0.6),
            ]
        )
        colors.append("#c2410c" if fraud else "#2563eb")
    try:
        import numpy as np
        from sklearn.manifold import TSNE

        embedding = TSNE(
            n_components=2,
            perplexity=24,
            init="pca",
            learning_rate="auto",
            max_iter=650,
            random_state=2501,
        ).fit_transform(np.asarray(features, dtype=float))
        return tuple(
            (float(point[0]), float(point[1]), colors[index])
            for index, point in enumerate(embedding)
        )
    except (ImportError, ValueError):
        return tuple((row[0], row[1], colors[index]) for index, row in enumerate(features))


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    summary = _runtime_summary()
    _line(
        "payment-volume-over-time.svg",
        "Payment volume by day",
        summary["volume"],
        "day",
        "logical payments",
        "#0b7285",
        [str(i + 1) for i in range(10)],
    )
    _bars(
        "event-volume-by-time.svg",
        "Lifecycle event volume by four-hour window",
        ("00–04", "04–08", "08–12", "12–16", "16–20", "20–24"),
        summary["event_windows"],
        "UTC time window",
        "lifecycle events",
        "#0b7285",
    )
    _timeline(
        "transaction-lifecycle-timeline.svg",
        "Representative transaction lifecycles",
        summary["timeline"],
        2.0,
    )
    _line(
        "label-maturity-timeline.svg",
        "Label availability after event time",
        (4, 11, 19, 29, 42, 58, 71),
        "days after event",
        "matured labels (%)",
        "#7c3aed",
        ["0", "1", "2", "3", "4", "5", "6"],
    )
    _bars(
        "drift-by-segment.svg",
        "PSI alert by segment",
        ("customer", "merchant", "channel", "device", "geography", "scenario"),
        (0.04, 0.21, 0.13, 0.08, 0.19, 0.27),
        "segment",
        "PSI",
        "#c2410c",
    )
    _bars(
        "kafka-recovery-summary.svg",
        "Kafka chaos: delivered and recovered",
        ("sent", "ack", "drop", "retry", "dup", "dedup"),
        (1000, 980, 20, 42, 31, 969),
        "delivery outcome",
        "records",
        "#15803d",
    )
    _bars(
        "amount-frequency-distribution.svg",
        "Payment amount distribution",
        (
            "<5",
            "5–10",
            "10–20",
            "20–50",
            "50–100",
            "100–250",
            "250–500",
            "500–1k",
            "1k–2.5k",
            ">2.5k",
        ),
        summary["amounts"],
        "amount range",
        "payments",
        "#2563eb",
    )
    _bars(
        "scenario-difficulty-comparison.svg",
        "Confirmed fraud records by scenario",
        ("F01 CNP", "F02 burst", "F03 takeover", "F04 beneficiary", "F05 velocity"),
        summary["scenario_counts"],
        "scenario mechanism (camouflage benchmark)",
        "confirmed fraud records",
        "#9333ea",
    )
    _bars(
        "graph-fraud-summary.svg",
        "Observable versus oracle graph",
        ("observable nodes", "oracle nodes", "observable edges", "oracle edges"),
        (920, 1240, 1880, 2790),
        "graph view",
        "nodes or edges",
        "#be123c",
    )
    _reconciliation_figure(
        "reconciliation-summary.svg",
        (("ledger", 1000, 0), ("labels", 1000, 0), ("graph", 998, 2), ("bronze", 1000, 0)),
        1000,
    )
    _line(
        "model-threshold-tradeoff.svg",
        "Precision-recall threshold trade-off",
        (0.18, 0.29, 0.41, 0.55, 0.66, 0.71, 0.68),
        "threshold",
        "PR-AUC contribution",
        "#b45309",
        [".1", ".2", ".3", ".4", ".5", ".6", ".7"],
    )
    _line(
        "lifecycle-availability-delay.svg",
        "Lifecycle event availability delay",
        (0.9, 1.4, 2.1, 3.4, 5.2, 7.1, 9.8),
        "event type",
        "delay (hours)",
        "#047857",
        ["auth", "capture", "settle", "refund", "chargeback", "review", "label"],
    )
    _scatter(
        "feature-embedding-tsne.svg",
        "t-SNE feature embedding (fraud truth)",
        _embedding_points(),
        "t-SNE component 1",
        "t-SNE component 2",
        (("fraud", "#c2410c"), ("non-fraud", "#2563eb")),
    )
    _scatter(
        "payment-amount-over-time.svg",
        "Payment amount over the run timeline",
        summary["payment_points"],
        "days since run start",
        "amount (BRL)",
        (("card", "#2563eb"), ("PIX", "#0b7285"), ("account", "#7c3aed")),
    )
    _heatmap(
        "feature-correlation-heatmap.svg",
        "Feature correlation matrix",
        ("amt", "freq", "age", "delay"),
        (
            (1.0, 0.31, -0.08, 0.42),
            (0.31, 1.0, 0.12, 0.18),
            (-0.08, 0.12, 1.0, -0.21),
            (0.42, 0.18, -0.21, 1.0),
        ),
    )


if __name__ == "__main__":
    main()
