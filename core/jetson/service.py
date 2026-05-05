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


def startup_db_init(db_handler, ip: str, port: int = 8080) -> None:
    """서버 기동 시 Jetson 정보를 DB에 upsert."""
    jetson_info = {
        "jetson_wp": "제1공장",
        "jetson_loc": "컨베이어 벨트 옆",
        "jetson_status": True,
        "ip_addr": ip,
        "port": port,
    }
    success = db_handler.init_jetson_info(jetson_info)
    if success:
        logger.info("[DB] Jetson 정보 업데이트 완료 | IP: %s", ip)
    else:
        logger.error("[DB] Jetson 정보 업데이트 실패")
