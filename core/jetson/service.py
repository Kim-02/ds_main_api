"""Jetson 등록 및 IP 탐지 서비스."""
import socket
import logging

logger = logging.getLogger(__name__)


def get_real_ip() -> str:
    """현재 할당된 진짜 로컬 IP 반환."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = "127.0.0.1"
    finally:
        s.close()
    return ip
