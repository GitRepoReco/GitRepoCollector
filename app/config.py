import os
import logging

from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Variable d'environnement manquante : {name}")
    return value


GITHUB_TOKEN: str = _require("GITHUB_TOKEN")

POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "github_data")
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")

PAGE_SIZE: int = int(os.getenv("PAGE_SIZE", "100"))
MAX_REPOSITORIES: int = int(os.getenv("MAX_REPOSITORIES", "0"))
POLL_INTERVAL_SECONDS: int = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))

# Seuils de stars pour les passes successives, du plus élevé au plus bas
STAR_TIERS: list[int] = sorted(
    [int(x.strip()) for x in os.getenv(
        "STAR_TIERS", "10000"
    ).split(",")],
    reverse=True,
)

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

GITHUB_GRAPHQL_URL: str = "https://api.github.com/graphql"
