import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "app.log"


def configurar_logger():
    logger = logging.getLogger("game-price-analytics")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    formato = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    arquivo = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
    arquivo.setFormatter(formato)

    console = logging.StreamHandler()
    console.setFormatter(formato)

    logger.addHandler(arquivo)
    logger.addHandler(console)

    return logger