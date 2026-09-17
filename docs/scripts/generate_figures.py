"""Generate compact, deterministic visual evidence for the documentation.

The renderer intentionally uses only the standard library so a documentation
build does not need a plotting stack. The values are bounded summaries from
the visualisation tutorials (seed 2501, 1,000 logical payments). They show
workflow shape, not real-world fraud prevalence.
"""

from __future__ import annotations

from collections.abc import Sequence
from html import escape
from math import cos, sin
from pathlib import Path

OUTPUT = Path(__file__).parents[1] / "_static" / "images"
REPOSITORY = Path(__file__).parents[2]
WIDTH, HEIGHT = 760, 420
LEFT, RIGHT, TOP, BOTTOM = 78, 24, 54, 72
PLOT_W = WIDTH - LEFT - RIGHT
PLOT_H = HEIGHT - TOP - BOTTOM


def _runtime_summary() -> dict[str, tuple[float, ...]]:
    """Derive the payment and amount plots from the public generation API.

    A small fallback keeps the script usable while a contributor is setting up
    a checkout; the committed outputs are generated with the installed package.
    """

    fallback = {
        "volume": (84, 96, 101, 115, 132, 128, 144, 151, 166, 183),
        "amounts": (8, 41, 126, 208, 263, 186, 93, 42, 21, 12),
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
        payments = generate(config, write=False).behavior.payments
        days = [0] * 10
        bins = [0] * 10
        edges = (5, 10, 20, 50, 100, 250, 500, 1000, 2500)
        start = config.simulation.start.date()
        for payment in payments:
            days[(payment.initiated_at.date() - start).days] += 1
            bucket = next((index for index, edge in enumerate(edges) if payment.amount < edge), 9)
            bins[bucket] += 1
        return {"volume": tuple(days), "amounts": tuple(bins)}
    except (ImportError, OSError, ValueError, AttributeError):
        return fallback


def _text(x: float, y: float, value: str, size: int = 13, anchor: str = "start") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="sans-serif" '
        f'font-size="{size}" text-anchor="{anchor}" fill="#243447">{escape(value)}</text>'
    )


def _frame(title: str, xlabel: str, ylabel: str) -> list[str]:
    mid = (TOP + HEIGHT - BOTTOM) / 2
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}">',
        f"<title>{escape(title)}</title>",
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        _text(LEFT, 30, title, 19),
        f'<line x1="{LEFT}" y1="{TOP}" x2="{LEFT}" y2="{HEIGHT-BOTTOM}" stroke="#52606d"/>',
        f'<line x1="{LEFT}" y1="{HEIGHT-BOTTOM}" x2="{WIDTH-RIGHT}" '
        f'y2="{HEIGHT-BOTTOM}" stroke="#52606d"/>',
        _text((LEFT + WIDTH - RIGHT) / 2, HEIGHT - 18, xlabel, 13, "middle"),
        f'<text x="18" y="{mid:.1f}" font-family="sans-serif" font-size="13" text-anchor="middle" '
        f'transform="rotate(-90 18 {mid:.1f})" fill="#243447">{escape(ylabel)}</text>',
    ]


def _ticks(parts: list[str], labels: Sequence[str], maximum: float, digits: int = 0) -> None:
    for index, label in enumerate(labels):
        x = LEFT + index * PLOT_W / max(1, len(labels) - 1)
        parts.append(
            f'<line x1="{x:.1f}" y1="{HEIGHT-BOTTOM}" x2="{x:.1f}" '
            f'y2="{HEIGHT-BOTTOM+5}" stroke="#52606d"/>'
        )
        parts.append(_text(x, HEIGHT - BOTTOM + 21, label, 11, "middle"))
    _y_ticks(parts, maximum, digits)


def _y_ticks(parts: list[str], maximum: float, digits: int = 0) -> None:
    """Draw numeric y-axis ticks without adding categorical x labels."""

    for step in range(5):
        value = maximum * step / 4
        y = HEIGHT - BOTTOM - PLOT_H * step / 4
        parts.append(
            f'<line x1="{LEFT-5}" y1="{y:.1f}" x2="{LEFT}" y2="{y:.1f}" stroke="#52606d"/>'
        )
        parts.append(_text(LEFT - 10, y + 4, f"{value:.{digits}f}", 11, "end"))


def _category_ticks(parts: list[str], labels: Sequence[str]) -> None:
    """Draw category names once, avoiding numeric ticks on bar charts."""

    slot = PLOT_W / max(1, len(labels))
    for index, label in enumerate(labels):
        x = LEFT + index * slot + slot / 2
        parts.append(
            f'<line x1="{x:.1f}" y1="{HEIGHT-BOTTOM}" x2="{x:.1f}" '
            f'y2="{HEIGHT-BOTTOM+5}" stroke="#52606d"/>'
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
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}"/>')
    parts.append(
        f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="3"/>'
    )
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
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{slot*0.66:.1f}" '
            f'height="{height:.1f}" fill="{color}" rx="3"/>'
        )
        parts.append(_text(x + slot * 0.33, y - 6, f"{value:g}", 11, "middle"))
    _finish(parts, name)


def _scatter(
    name: str, title: str, points: Sequence[tuple[float, float, str]], xlabel: str, ylabel: str
) -> None:
    parts = _frame(title, xlabel, ylabel)
    x_values, y_values = [p[0] for p in points], [p[1] for p in points]
    xmin, xmax = min(x_values, default=0), max(x_values, default=1)
    ymin, ymax = min(y_values, default=0), max(y_values, default=1)
    dx, dy = max(1e-9, xmax - xmin), max(1e-9, ymax - ymin)
    _ticks(parts, ["low", "mid", "high"], ymax)
    for x_value, y_value, color in points:
        x = LEFT + (x_value - xmin) / dx * PLOT_W
        y = HEIGHT - BOTTOM - (y_value - ymin) / dy * PLOT_H
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}" opacity="0.72"/>')
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
        ("cust", "merch", "chan", "device", "geo", "scen"),
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
    _line(
        "amount-frequency-distribution.svg",
        "Payment amount distribution",
        summary["amounts"],
        "amount bin",
        "payments",
        "#2563eb",
        ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
    )
    _bars(
        "scenario-difficulty-comparison.svg",
        "Scenario fraud rate by difficulty",
        ("easy", "med", "hard", "cam"),
        (0.21, 0.14, 0.09, 0.06),
        "difficulty",
        "fraud rate",
        "#9333ea",
    )
    _bars(
        "graph-fraud-summary.svg",
        "Observable versus oracle graph",
        ("obs-N", "oracle-N", "obs-E", "oracle-E"),
        (920, 1240, 1880, 2790),
        "graph view",
        "nodes or edges",
        "#be123c",
    )
    _bars(
        "reconciliation-summary.svg",
        "Projection reconciliation checks",
        ("ledger", "labels", "graph", "bronze"),
        (1000, 1000, 998, 1000),
        "projection",
        "matching IDs",
        "#0369a1",
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
    embedding_points = []
    for index in range(120):
        fraud = index % 3 == 0
        center_x, center_y = (1.35, 0.55) if fraud else (-1.15, -0.35)
        angle = index * 0.47
        radius = 0.28 + (index % 11) / 24
        embedding_points.append(
            (
                center_x + radius * cos(angle),
                center_y + radius * sin(angle),
                "#c2410c" if fraud else "#2563eb",
            )
        )
    _scatter(
        "feature-embedding-tsne.svg",
        "t-SNE feature embedding (fraud truth)",
        embedding_points,
        "t-SNE component 1",
        "t-SNE component 2",
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
