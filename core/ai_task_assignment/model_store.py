from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from django.conf import settings


@dataclass(frozen=True)
class ModelMeta:
    feature_cols: list[str]
    cat_cols: list[str]

    @property
    def cat_idx(self) -> list[int]:
        return [self.feature_cols.index(c) for c in self.cat_cols if c in self.feature_cols]


def _default_model_path() -> Path:
    base_dir = getattr(settings, "BASE_DIR", Path("."))
    return Path(getattr(settings, "TASK_ASSIGNER_MODEL_PATH", base_dir / "ml_artifacts/models/task_assigner_v1.cbm"))


def _default_meta_path() -> Path:
    base_dir = getattr(settings, "BASE_DIR", Path("."))
    return Path(getattr(settings, "TASK_ASSIGNER_META_PATH", base_dir / "ml_artifacts/models/task_assigner_v1.meta.json"))


def _read_meta(meta_path: Path) -> ModelMeta:
    data = json.loads(meta_path.read_text(encoding="utf-8"))
    feature_cols = list(data.get("feature_cols") or [])
    cat_cols = list(data.get("cat_cols") or [])
    if not feature_cols:
        raise ValueError("meta.json has empty feature_cols")
    return ModelMeta(feature_cols=feature_cols, cat_cols=cat_cols)


@lru_cache(maxsize=1)
def load_model_and_meta():
    """
    Loads CatBoost model + meta once per process (Celery worker / runserver).
    If something goes wrong, raise – caller will fallback to heuristic.
    """
    model_path = _default_model_path()
    meta_path = _default_meta_path()

    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")
    if not meta_path.exists():
        raise FileNotFoundError(f"Meta file not found: {meta_path}")

    meta = _read_meta(meta_path)

    # Lazy import so проект не падает при import core.services.tasks,
    # если catboost ещё не установлен.
    from catboost import CatBoostRegressor  # type: ignore

    model = CatBoostRegressor()
    model.load_model(str(model_path))
    return model, meta
