"""YOLO runtime setup helpers for Jetson/TensorRT startup."""
from __future__ import annotations

import importlib
import logging
import os
import sys
from pathlib import Path


logger = logging.getLogger(__name__)

_CONFIGURED = False


def configure_ultralytics_runtime() -> None:
    """Make Ultralytics startup deterministic in the API server.

    Ultralytics tries to install missing packages automatically by default. On
    Jetson this can block server startup while it attempts to fetch TensorRT
    wheels. The API should fail fast with a clear error instead.
    """
    global _CONFIGURED

    os.environ.setdefault("YOLO_AUTOINSTALL", "False")
    os.environ.setdefault("ULTRALYTICS_SKIP_REQUIREMENTS_CHECKS", "1")

    added_paths = []
    for path in _jetson_system_package_paths():
        if path.exists():
            path_text = str(path)
            if path_text not in sys.path:
                sys.path.append(path_text)
                added_paths.append(path_text)

    _disable_ultralytics_autoinstall_if_imported()

    if not _CONFIGURED:
        logger.info(
            "[YOLO_RUNTIME] configured autoinstall=%s skip_requirements=%s added_paths=%s",
            os.environ.get("YOLO_AUTOINSTALL"),
            os.environ.get("ULTRALYTICS_SKIP_REQUIREMENTS_CHECKS"),
            added_paths,
        )
        _CONFIGURED = True


def ensure_tensorrt_available(model_path: str) -> str | None:
    """Check TensorRT Python bindings before loading a .engine model."""
    if not str(model_path).lower().endswith(".engine"):
        return None

    configure_ultralytics_runtime()

    try:
        tensorrt = importlib.import_module("tensorrt")
    except Exception as exc:
        raise RuntimeError(
            "YOLO TensorRT engine(.engine)을 로드하려면 tensorrt Python 모듈이 필요합니다. "
            "Jetson에서 python3-libnvinfer가 설치되어 있는지 확인하고, "
            "venv를 만들 때 --system-site-packages를 사용했는지 확인하세요."
        ) from exc

    version = str(getattr(tensorrt, "__version__", "unknown"))
    logger.info(
        "[YOLO_RUNTIME] TensorRT Python binding ready version=%s module=%s",
        version,
        getattr(tensorrt, "__file__", "unknown"),
    )
    return version


def _jetson_system_package_paths() -> list[Path]:
    major = sys.version_info.major
    minor = sys.version_info.minor
    return [
        Path(f"/usr/lib/python{major}.{minor}/dist-packages"),
        Path("/usr/lib/python3/dist-packages"),
    ]


def _disable_ultralytics_autoinstall_if_imported() -> None:
    try:
        import ultralytics.utils as ultralytics_utils

        ultralytics_utils.AUTOINSTALL = False
    except Exception:
        pass

    try:
        import ultralytics.utils.checks as ultralytics_checks

        ultralytics_checks.AUTOINSTALL = False
    except Exception:
        pass
