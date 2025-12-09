# src/models/xgb_signal_model.py
from __future__ import annotations

import xgboost as xgb
import pandas as pd


class XGBSignalModel:
    """
    XGBoost model for ranking symbols by expected next-day return.

    - Uses regression on clipped forward return (next_ret).
    - GPU training via device="cuda" (change to "cpu" if ever needed).
    """

    def __init__(self, params: dict | None = None, num_boost_round: int = 300):
        default_params = {
            "objective": "reg:squarederror",
    "max_depth": 5,
    "eta": 0.03,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "tree_method": "hist",
    "device": "cuda",
    "max_bin": 512,
    "eval_metric": "rmse",
        }
        self.params = {**default_params, **(params or {})}
        self.num_boost_round = num_boost_round
        self.model: xgb.Booster | None = None

    def fit(self, X: pd.DataFrame, y: pd.Series) -> None:
        """Train the model on features X and target y."""
        dtrain = xgb.DMatrix(X, label=y)
        self.model = xgb.train(
            params=self.params,
            dtrain=dtrain,
            num_boost_round=self.num_boost_round,
        )

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """Return scores as a Series aligned with X.index."""
        if self.model is None:
            raise RuntimeError("Model not fitted yet")

        dtest = xgb.DMatrix(X)
        preds = self.model.predict(dtest)
        return pd.Series(preds, index=X.index, name="ml_score")
