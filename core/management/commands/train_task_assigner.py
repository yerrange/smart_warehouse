from __future__ import annotations

import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Train ML model for task assignment (predict expected minutes for (task, employee))."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dataset",
            required=True,
            help="Path to CSV dataset produced by simulate_history (e.g. ml_data/sim_dataset.csv)",
        )
        parser.add_argument(
            "--model-out",
            default="ml_artifacts/models/task_assigner_v1.cbm",
            help="Where to save CatBoost model (.cbm)",
        )
        parser.add_argument(
            "--meta-out",
            default="ml_artifacts/models/task_assigner_v1.meta.json",
            help="Where to save model metadata (.json)",
        )
        parser.add_argument(
            "--test-size",
            type=float,
            default=0.2,
            help="Share of tasks to use for validation (group split by task_id).",
        )
        parser.add_argument(
            "--seed",
            type=int,
            default=42,
            help="Random seed for split and training.",
        )
        parser.add_argument(
            "--iters",
            type=int,
            default=600,
            help="CatBoost iterations (trees).",
        )
        parser.add_argument(
            "--depth",
            type=int,
            default=8,
            help="CatBoost tree depth.",
        )
        parser.add_argument(
            "--lr",
            type=float,
            default=0.08,
            help="CatBoost learning rate.",
        )

    def handle(self, *args, **opts):
        dataset_path = Path(opts["dataset"])
        if not dataset_path.exists():
            raise CommandError(f"Dataset file not found: {dataset_path}")

        # Lazy imports with clear errors
        try:
            import pandas as pd
        except Exception as e:
            raise CommandError("pandas is required. Install: pip install pandas") from e

        try:
            from catboost import CatBoostRegressor, Pool
        except Exception as e:
            raise CommandError("catboost is required. Install: pip install catboost") from e

        try:
            from sklearn.model_selection import GroupShuffleSplit
            from sklearn.metrics import mean_absolute_error, mean_squared_error
        except Exception as e:
            raise CommandError("scikit-learn is required. Install: pip install scikit-learn") from e

        df = pd.read_csv(dataset_path)

        required_cols = {
            "task_id",
            "employee_id",
            "task_type",
            "priority",
            "difficulty",
            "estimated_minutes",
            "task_age_minutes",
            "emp_task_assigned_count",
            "emp_task_completed_count",
            "emp_shift_score",
            "minutes_into_shift",
            "label_true_minutes",
            "was_assigned",
        }
        missing = required_cols - set(df.columns)
        if missing:
            raise CommandError(f"Dataset is missing columns: {sorted(missing)}")

        # -----------------------------
        # Features / target
        # -----------------------------
        target_col = "label_true_minutes"

        # Важно: was_assigned нам для обучения не нужен (это “что выбрал симулятор”).
        # Мы учим модель предсказывать время выполнения для любой пары (task, employee).
        feature_cols = [
            "task_type",  # categorical
            "priority",
            "difficulty",
            "estimated_minutes",
            "task_age_minutes",
            "emp_task_assigned_count",
            "emp_task_completed_count",
            "emp_shift_score",
            "minutes_into_shift",
        ]
        cat_cols = ["task_type"]

        X = df[feature_cols].copy()
        y = df[target_col].astype(float)
        groups = df["task_id"]

        # -----------------------------
        # Split by task_id (no leakage)
        # -----------------------------
        test_size = float(opts["test_size"])
        if not (0.05 <= test_size <= 0.5):
            raise CommandError("--test-size must be between 0.05 and 0.5")

        splitter = GroupShuffleSplit(
            n_splits=1,
            test_size=test_size,
            random_state=int(opts["seed"]),
        )
        train_idx, val_idx = next(splitter.split(X, y, groups=groups))

        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        # CatBoost wants categorical indices
        cat_idx = [feature_cols.index(c) for c in cat_cols]

        train_pool = Pool(X_train, y_train, cat_features=cat_idx)
        val_pool = Pool(X_val, y_val, cat_features=cat_idx)

        # -----------------------------
        # Train
        # -----------------------------
        model = CatBoostRegressor(
            loss_function="MAE",
            random_seed=int(opts["seed"]),
            iterations=int(opts["iters"]),
            depth=int(opts["depth"]),
            learning_rate=float(opts["lr"]),
            eval_metric="MAE",
            verbose=100,
        )
        model.fit(train_pool, eval_set=val_pool, use_best_model=True)

        # -----------------------------
        # Evaluate
        # -----------------------------
        val_pred = model.predict(X_val)

        mae = mean_absolute_error(y_val, val_pred)
        rmse = mean_squared_error(y_val, val_pred) ** 0.5  # <-- FIX

        # baseline: always predict mean of train
        baseline_pred = [float(y_train.mean())] * len(y_val)
        baseline_mae = mean_absolute_error(y_val, baseline_pred)
        baseline_rmse = mean_squared_error(y_val, baseline_pred) ** 0.5  # <-- FIX

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Validation metrics (group split by task_id):"))
        self.stdout.write(f"  MAE  = {mae:.3f} minutes")
        self.stdout.write(f"  RMSE = {rmse:.3f} minutes")
        self.stdout.write(self.style.WARNING("Baseline (predict train mean):"))
        self.stdout.write(f"  MAE  = {baseline_mae:.3f} minutes")
        self.stdout.write(f"  RMSE = {baseline_rmse:.3f} minutes")

        # -----------------------------
        # Save artifacts
        # -----------------------------
        model_out = Path(opts["model_out"])
        model_out.parent.mkdir(parents=True, exist_ok=True)
        model.save_model(str(model_out))

        meta_out = Path(opts["meta_out"])
        meta_out.parent.mkdir(parents=True, exist_ok=True)
        meta = {
            "model_type": "CatBoostRegressor",
            "loss_function": "MAE",
            "feature_cols": feature_cols,
            "cat_cols": cat_cols,
            "dataset": str(dataset_path),
            "seed": int(opts["seed"]),
            "iterations": int(model.tree_count_),
            "metrics": {
                "mae": float(mae),
                "rmse": float(rmse),
                "baseline_mae": float(baseline_mae),
                "baseline_rmse": float(baseline_rmse),
            },
        }
        meta_out.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"Saved model to: {model_out}"))
        self.stdout.write(self.style.SUCCESS(f"Saved metadata to: {meta_out}"))
