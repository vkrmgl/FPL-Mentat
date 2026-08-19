"""Metrics for component models.

Two principles, both learned the hard way on this project:

1. MAE is not a decision metric here. FPL points for a regular starter have
   median 2, and MAE is minimised by the conditional median - a constant
   predictor of 2.0 ties a trained LightGBM exactly (1.957 each, measured over
   9,955 held-out rows). Roughly a fifth of all points come from hauls of 10+
   occurring about 4% of the time, and that tail is where every captaincy and
   transfer decision lives. Report RMSE and ranking; keep MAE for reference.

2. Calibration matters more than accuracy for anything feeding the simulation.
   If the appearance model says 70%, it has to happen 70% of the time or every
   sampled gameweek is biased. Gradient-boosted trees are routinely
   miscalibrated out of the box, so this module measures it explicitly rather
   than assuming.
"""

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
)


def regression_metrics(y_true, y_pred, groups=None, haul_threshold=10, k=5):
    """RMSE and ranking quality. RMSE is primary."""
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    mask = ~(np.isnan(y_true) | np.isnan(y_pred))
    y_true, y_pred = y_true[mask], y_pred[mask]

    out = {
        "n": int(mask.sum()),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "spearman": float(spearmanr(y_true, y_pred).statistic)
        if len(np.unique(y_pred)) > 1 else float("nan"),
    }

    if groups is not None:
        g = pd.DataFrame({
            "g": np.asarray(groups)[mask], "y": y_true, "p": y_pred,
        })
        hits = total = 0
        for _, chunk in g.groupby("g"):
            if len(chunk) < 2 * k:
                continue
            top = chunk.nlargest(k, "p")
            hits += int((top["y"] >= haul_threshold).sum())
            total += len(top)
        out[f"haul_at_{k}"] = hits / total if total else float("nan")
    return out


def classification_metrics(y_true, proba, labels=None):
    """Log loss, Brier and accuracy for a multiclass appearance model."""
    y_true = np.asarray(y_true)
    labels = labels if labels is not None else np.unique(y_true)
    pred = np.asarray(labels)[proba.argmax(axis=1)]

    out = {
        "n": len(y_true),
        "log_loss": float(log_loss(y_true, proba, labels=list(labels))),
        "accuracy": float((pred == y_true).mean()),
    }
    for i, label in enumerate(labels):
        out[f"brier_class{label}"] = float(
            brier_score_loss((y_true == label).astype(int), proba[:, i])
        )
    return out


def calibration_table(y_binary, prob, n_bins=10):
    """Reliability table: predicted probability against observed frequency."""
    y_binary = np.asarray(y_binary, dtype=float)
    prob = np.asarray(prob, dtype=float)

    edges = np.linspace(0, 1, n_bins + 1)
    idx = np.clip(np.digitize(prob, edges[1:-1]), 0, n_bins - 1)

    rows = []
    for b in range(n_bins):
        sel = idx == b
        if not sel.any():
            continue
        rows.append({
            "bin": f"{edges[b]:.1f}-{edges[b + 1]:.1f}",
            "n": int(sel.sum()),
            "predicted": float(prob[sel].mean()),
            "observed": float(y_binary[sel].mean()),
            "gap": float(prob[sel].mean() - y_binary[sel].mean()),
        })
    return pd.DataFrame(rows)


def expected_calibration_error(y_binary, prob, n_bins=10):
    """Weighted mean gap between predicted and observed frequency.

    0 is perfect. Anything above ~0.02 will visibly bias a simulation that
    samples from these probabilities.
    """
    table = calibration_table(y_binary, prob, n_bins)
    if table.empty:
        return float("nan")
    weights = table["n"] / table["n"].sum()
    return float((weights * table["gap"].abs()).sum())


def print_metrics(title, metrics):
    print(f"  {title}")
    for key, value in metrics.items():
        if key == "n":
            continue
        formatted = f"{value:.4f}" if isinstance(value, float) else value
        print(f"    {key:<22} {formatted}")
