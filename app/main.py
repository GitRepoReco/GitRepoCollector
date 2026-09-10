import logging
import signal
import sys
import types

from config import LOG_LEVEL
from database.schema import initialize_schemas
from services.historical import HistoricalCollector
from services.watcher import WatcherService


def _configure_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def _handle_shutdown(signum: int, frame: types.FrameType | None) -> None:
    logging.getLogger(__name__).info("Signal %d reçu — arrêt propre.", signum)
    sys.exit(0)


def main() -> None:
    _configure_logging()
    logger = logging.getLogger(__name__)

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    logger.info("=== GitHub GraphQL Collector démarré ===")

    logger.info("Initialisation des schemas PostgreSQL...")
    initialize_schemas()

    logger.info("Démarrage de la collecte historique...")
    HistoricalCollector().collect_all()

    logger.info("Démarrage du mode surveillance continue...")
    WatcherService().run()


if __name__ == "__main__":
    main()
