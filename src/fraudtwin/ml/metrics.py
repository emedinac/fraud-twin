"""Deterministic ranking metrics shared by ML evaluation paths."""

from collections.abc import Sequence


def auc(labels: Sequence[int], scores: Sequence[float], row_ids: Sequence[str]) -> float | None:
    positives = sum(labels)
    negatives = len(labels) - positives
    if not positives or not negatives:
        return None
    ordered = sorted(zip(scores, labels, row_ids, strict=True), key=lambda item: (item[0], item[2]))
    rank_sum = sum(rank for rank, (_, label, _) in enumerate(ordered, 1) if label)
    return (rank_sum - positives * (positives + 1) / 2) / (positives * negatives)


def pr_auc(labels: Sequence[int], scores: Sequence[float], row_ids: Sequence[str]) -> float | None:
    positives = sum(labels)
    if not positives:
        return None
    ordered = sorted(
        zip(scores, labels, row_ids, strict=True), key=lambda item: (-item[0], item[2])
    )
    area = 0.0
    found = 0
    previous_recall = 0.0
    for index, (_, label, _) in enumerate(ordered, 1):
        found += label
        recall = found / positives
        area += (recall - previous_recall) * (found / index)
        previous_recall = recall
    return area
