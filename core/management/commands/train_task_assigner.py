from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from core.ai_task_assignment.features import CAT_COLS, FEATURE_COLS


TARGET_COL = "label_true_minutes"
GROUP_COL = "task_id"
FEATURE_VERSION = "v1_employee_task_profiles"


class Command(BaseCommand):
    help = (
        "Train CatBoost task assignment model from a training-ready "
        "execution_dataset.csv exported by export_execution_dataset."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dataset",
            default="ml_data/execution_dataset.csv",
            help="Path to training-ready execution dataset CSV.",
        )
        parser.add_argument(
            "--model-out",
            default="ml_artifacts/models/task_assigner_v1.cbm",
            help="Where to save CatBoost model (.cbm).",
        )
        parser.add_argument(
            "--meta-out",
            default="ml_artifacts/models/task_assigner_v1.meta.json",
            help="Where to save model metadata (.json).",
        )
        parser.add_argument(
            "--test-size",
            type=float,
            default=0.2,
            help="Share of task_id groups to use for validation.",
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
            default=800,
            help="CatBoost iterations.",
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
            default=0.06,
            help="CatBoost learning rate.",
        )
        parser.add_argument(
            "--min-rows",
            type=int,
            default=20,
            help="Minimum number of valid training rows.",
        )

    def handle(self, *args, **options):
        dataset_path = Path(options["dataset"])
        if not dataset_path.exists():
            raise CommandError(f"Dataset file not found: {dataset_path}")

        try:
            import pandas as pd
        except Exception as exc:
            raise CommandError("pandas is required. Install: pip install pandas") from exc

        try:
            from catboost import CatBoostRegressor, Pool
        except Exception as exc:
            raise CommandError("catboost is required. Install: pip install catboost") from exc

        try:
            from sklearn.metrics import mean_absolute_error, mean_squared_error
            from sklearn.model_selection import GroupShuffleSplit
        except Exception as exc:
            raise CommandError("scikit-learn is required. Install: pip install scikit-learn") from exc

        df_raw = pd.read_csv(dataset_path)
        if df_raw.empty:
            raise CommandError("Dataset is empty")

        self._validate_dataset_columns(df_raw)
        df = self._prepare_training_frame(df_raw)

        rows_before_target_filter = len(df)
        df = df[df[TARGET_COL].notna() & (df[TARGET_COL] > 0)].copy()
        rows_dropped_by_target = rows_before_target_filter - len(df)

        min_rows = int(options["min_rows"])
        if len(df) < min_rows:
            raise CommandError(
                f"Too few valid training rows: {len(df)}. "
                f"Minimum required: {min_rows}. Export and simulate more completed shifts."
            )

        unique_tasks = df[GROUP_COL].nunique()
        if unique_tasks < 2:
            raise CommandError(
                f"Need at least two distinct {GROUP_COL} groups for validation split."
            )

        test_size = float(options["test_size"])
        if not (0.05 <= test_size <= 0.5):
            raise CommandError("--test-size must be between 0.05 and 0.5")

        X = df[FEATURE_COLS].copy()
        y = df[TARGET_COL].astype(float)
        groups = df[GROUP_COL]

        splitter = GroupShuffleSplit(
            n_splits=1,
            test_size=test_size,
            random_state=int(options["seed"]),
        )
        train_idx, val_idx = next(splitter.split(X, y, groups=groups))

        X_train = X.iloc[train_idx].copy()
        X_val = X.iloc[val_idx].copy()
        y_train = y.iloc[train_idx].copy()
        y_val = y.iloc[val_idx].copy()

        if X_train.empty or X_val.empty:
            raise CommandError(
                "Validation split produced empty train or validation subset. "
                "Export more task groups or change --test-size."
            )

        cat_feature_indices = [
            FEATURE_COLS.index(col)
            for col in CAT_COLS
            if col in FEATURE_COLS
        ]

        train_pool = Pool(X_train, y_train, cat_features=cat_feature_indices)
        val_pool = Pool(X_val, y_val, cat_features=cat_feature_indices)

        model = CatBoostRegressor(
            loss_function="MAE",
            eval_metric="MAE",
            random_seed=int(options["seed"]),
            iterations=int(options["iters"]),
            depth=int(options["depth"]),
            learning_rate=float(options["lr"]),
            verbose=100,
            allow_writing_files=False,
        )
        model.fit(train_pool, eval_set=val_pool, use_best_model=True)

        val_pred = model.predict(X_val)
        mae = float(mean_absolute_error(y_val, val_pred))
        rmse = float(mean_squared_error(y_val, val_pred) ** 0.5)

        baseline_pred = [float(y_train.mean())] * len(y_val)
        baseline_mae = float(mean_absolute_error(y_val, baseline_pred))
        baseline_rmse = float(mean_squared_error(y_val, baseline_pred) ** 0.5)

        train_task_groups = int(df.iloc[train_idx][GROUP_COL].nunique())
        val_task_groups = int(df.iloc[val_idx][GROUP_COL].nunique())

        feature_importance = self._feature_importance(model)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Training dataset accepted:"))
        self.stdout.write(f"  Dataset path             = {dataset_path}")
        self.stdout.write(f"  Rows in CSV              = {len(df_raw)}")
        self.stdout.write(f"  Valid training rows      = {len(df)}")
        self.stdout.write(f"  Rows dropped by target   = {rows_dropped_by_target}")
        self.stdout.write(f"  Task groups              = {unique_tasks}")
        self.stdout.write(f"  Feature columns          = {len(FEATURE_COLS)}")
        self.stdout.write(f"  Categorical columns      = {', '.join(CAT_COLS)}")

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Validation metrics (group split by task_id):"))
        self.stdout.write(f"  Train rows               = {len(X_train)}")
        self.stdout.write(f"  Validation rows          = {len(X_val)}")
        self.stdout.write(f"  Train task groups        = {train_task_groups}")
        self.stdout.write(f"  Validation task groups   = {val_task_groups}")
        self.stdout.write(f"  MAE                      = {mae:.3f} minutes")
        self.stdout.write(f"  RMSE                     = {rmse:.3f} minutes")
        self.stdout.write(self.style.WARNING("Baseline (predict train mean):"))
        self.stdout.write(f"  MAE                      = {baseline_mae:.3f} minutes")
        self.stdout.write(f"  RMSE                     = {baseline_rmse:.3f} minutes")

        if feature_importance:
            self.stdout.write("")
            self.stdout.write(self.style.NOTICE("Top feature importance:"))
            for item in feature_importance[:10]:
                self.stdout.write(f"  {item['feature']}: {item['importance']:.4f}")

        model_out = Path(options["model_out"])
        model_out.parent.mkdir(parents=True, exist_ok=True)
        model.save_model(str(model_out))

        meta = {
            "model_type": "CatBoostRegressor",
            "feature_version": FEATURE_VERSION,
            "feature_cols": FEATURE_COLS,
            "cat_cols": CAT_COLS,
            "target_col": TARGET_COL,
            "group_col": GROUP_COL,
            "dataset": str(dataset_path),
            "seed": int(options["seed"]),
            "params": {
                "loss_function": "MAE",
                "eval_metric": "MAE",
                "iterations_requested": int(options["iters"]),
                "iterations_used": int(getattr(model, "tree_count_", 0) or 0),
                "depth": int(options["depth"]),
                "learning_rate": float(options["lr"]),
                "test_size": test_size,
            },
            "training": {
                "rows_in_csv": int(len(df_raw)),
                "valid_training_rows": int(len(df)),
                "rows_dropped_by_target": int(rows_dropped_by_target),
                "task_groups": int(unique_tasks),
                "train_rows": int(len(X_train)),
                "validation_rows": int(len(X_val)),
                "train_task_groups": train_task_groups,
                "validation_task_groups": val_task_groups,
            },
            "target_stats": self._describe_target(y),
            "metrics": {
                "mae": mae,
                "rmse": rmse,
                "baseline_mae": baseline_mae,
                "baseline_rmse": baseline_rmse,
            },
            "feature_importance": feature_importance,
        }

        meta_out = Path(options["meta_out"])
        meta_out.parent.mkdir(parents=True, exist_ok=True)
        meta_out.write_text(
            json.dumps(meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"Saved model to: {model_out}"))
        self.stdout.write(self.style.SUCCESS(f"Saved metadata to: {meta_out}"))

    def _validate_dataset_columns(self, df) -> None:
        required_cols = set(FEATURE_COLS) | set(CAT_COLS) | {TARGET_COL, GROUP_COL}
        missing = sorted(required_cols - set(df.columns))

        if missing:
            raise CommandError(
                "Dataset is not training-ready. Missing columns: "
                f"{missing}. Run export_execution_dataset again."
            )

    def _prepare_training_frame(self, df):
        import pandas as pd

        df = df.copy()

        for col in CAT_COLS:
            df[col] = df[col].fillna("unknown").astype(str)

        numeric_feature_cols = [col for col in FEATURE_COLS if col not in CAT_COLS]
        numeric_cols = numeric_feature_cols + [TARGET_COL]

        invalid_numeric: dict[str, int] = {}
        for col in numeric_cols:
            before_na = int(df[col].isna().sum())
            converted = pd.to_numeric(df[col], errors="coerce")
            after_na = int(converted.isna().sum())
            created_na = max(0, after_na - before_na)
            if created_na:
                invalid_numeric[col] = created_na
            df[col] = converted

        if invalid_numeric:
            raise CommandError(
                "Dataset contains non-numeric values in numeric columns: "
                f"{invalid_numeric}"
            )

        missing_feature_values = {
            col: int(df[col].isna().sum())
            for col in FEATURE_COLS
            if int(df[col].isna().sum()) > 0
        }
        if missing_feature_values:
            raise CommandError(
                "Dataset contains missing values in model feature columns: "
                f"{missing_feature_values}"
            )

        infinite_feature_values = {
            col: int((~df[col].map(_is_finite)).sum())
            for col in numeric_feature_cols
            if int((~df[col].map(_is_finite)).sum()) > 0
        }
        target_infinite_count = int(
            df[TARGET_COL].notna().map(bool).sum()
            - df.loc[df[TARGET_COL].notna(), TARGET_COL].map(_is_finite).sum()
        )
        if target_infinite_count:
            infinite_feature_values[TARGET_COL] = target_infinite_count

        if infinite_feature_values:
            raise CommandError(
                "Dataset contains infinite values in numeric columns: "
                f"{infinite_feature_values}"
            )

        df[GROUP_COL] = df[GROUP_COL].fillna("unknown").astype(str)

        return df

    def _feature_importance(self, model) -> list[dict[str, Any]]:
        try:
            values = model.get_feature_importance()
        except Exception:
            return []

        result = [
            {
                "feature": feature,
                "importance": float(importance),
            }
            for feature, importance in zip(FEATURE_COLS, values)
        ]
        result.sort(key=lambda item: item["importance"], reverse=True)
        return result

    def _describe_target(self, y) -> dict[str, float]:
        return {
            "min": float(y.min()),
            "max": float(y.max()),
            "mean": float(y.mean()),
            "median": float(y.median()),
            "std": float(y.std(ddof=0)),
        }


def _is_finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except Exception:
        return False
