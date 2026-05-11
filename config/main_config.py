"""Main runtime configuration.

프로젝트의 모든 런타임 기준값은 config.settings.Settings와 .env에서 관리한다.
이 모듈은 main_config 이름으로 설정을 확인하고 싶을 때 쓰는 호환 진입점이다.
"""

from .settings import Settings, settings

__all__ = ["Settings", "settings"]
