import logging
import time
from datetime import datetime, timezone
from typing import Generator

import requests
import requests.models

from config import GITHUB_GRAPHQL_URL, GITHUB_TOKEN, PAGE_SIZE

logger = logging.getLogger(__name__)

_TRANSIENT_STATUS_CODES = {500, 502, 503, 504}
_MAX_RETRIES = 5

# Backoff exponentiel plafonné à 120s, conforme aux recommandations GitHub
def _backoff(attempt: int) -> int:
    return min(10 * (2 ** attempt), 120)


def _rate_limit_from_headers(response: requests.models.Response) -> dict[str, int | str | None]:
    """Extrait les infos de rate limit depuis les headers HTTP (recommandé par GitHub)."""
    h = response.headers
    limit:       int = int(h.get("x-ratelimit-limit",     0) or 0)
    remaining:   int = int(h.get("x-ratelimit-remaining", -1) or -1)
    used:        int = int(h.get("x-ratelimit-used",       0) or 0)
    reset_epoch: int = int(h.get("x-ratelimit-reset",     0) or 0)

    reset_at: str | None = None
    if reset_epoch:
        reset_at = datetime.fromtimestamp(reset_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {"limit": limit, "remaining": remaining, "used": used, "resetAt": reset_at}


def _wait_for_reset(reset_at_iso: str | None, fallback_seconds: int = 60) -> int:
    """Calcule le nombre de secondes à attendre jusqu'au reset."""
    if reset_at_iso:
        reset_dt = datetime.fromisoformat(reset_at_iso.replace("Z", "+00:00"))
        wait = max(0, (reset_dt - datetime.now(timezone.utc)).total_seconds()) + 5
        return int(wait)
    return fallback_seconds


class GitHubGraphQLClient:

    def __init__(self) -> None:
        self._headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Content-Type": "application/json",
        }

    # ----------------------------------------------------------
    # Méthode principale : exécute une requête avec retry
    # ----------------------------------------------------------

    def execute(
        self,
        query: str,
        variables: dict[str, object],
        operation_name: str = "unknown",
    ) -> tuple[dict, dict]:
        """Retourne (data, rate_limit_info). Lève une exception en cas d'échec."""

        for attempt in range(_MAX_RETRIES + 1):
            try:
                response = requests.post(
                    GITHUB_GRAPHQL_URL,
                    json={"query": query, "variables": variables},
                    headers=self._headers,
                    timeout=60,
                )

                # --- Secondary rate limit (doc : 200 ou 403 + retry-after) ---
                if response.status_code == 403:
                    retry_after = response.headers.get("retry-after")
                    if retry_after:
                        wait = int(retry_after)
                        logger.warning(
                            "[%s] Secondary rate limit — retry-after: %ds",
                            operation_name, wait,
                        )
                    elif response.headers.get("x-ratelimit-remaining") == "0":
                        reset_at = _rate_limit_from_headers(response).get("resetAt")
                        wait = _wait_for_reset(reset_at)
                        logger.warning(
                            "[%s] Primary rate limit épuisé (403) — attente %ds",
                            operation_name, wait,
                        )
                    else:
                        wait = 60
                        logger.warning("[%s] HTTP 403 — attente %ds", operation_name, wait)

                    if attempt < _MAX_RETRIES:
                        time.sleep(wait)
                        continue
                    raise RuntimeError(f"[{operation_name}] HTTP 403 après {_MAX_RETRIES + 1} tentatives")

                # --- Erreurs serveur transitoires (502, 503, 504, 500) ---
                if response.status_code in _TRANSIENT_STATUS_CODES:
                    if attempt < _MAX_RETRIES:
                        wait = _backoff(attempt)
                        logger.warning(
                            "[%s] HTTP %s — retry %d/%d dans %ds",
                            operation_name,
                            response.status_code,
                            attempt + 1,
                            _MAX_RETRIES,
                            wait,
                        )
                        time.sleep(wait)
                        continue
                    raise RuntimeError(
                        f"[{operation_name}] HTTP {response.status_code} "
                        f"après {_MAX_RETRIES + 1} tentatives"
                    )

                if response.status_code != 200:
                    raise RuntimeError(
                        f"[{operation_name}] HTTP {response.status_code}\n"
                        f"{response.text}"
                    )

                result = response.json()

                if "errors" in result:
                    raise RuntimeError(
                        f"[{operation_name}] Erreur GraphQL : {result['errors']}"
                    )

                # --- Fusionner headers (priorité) + champ GraphQL (pour cost) ---
                header_rl = _rate_limit_from_headers(response)
                gql_rl    = result.get("data", {}).get("rateLimit") or {}
                rate_limit = {
                    "limit":     header_rl["limit"]     or gql_rl.get("limit"),
                    "remaining": header_rl["remaining"] if header_rl["remaining"] >= 0 else gql_rl.get("remaining"),
                    "used":      header_rl["used"]      or gql_rl.get("used"),
                    "cost":      gql_rl.get("cost"),
                    "resetAt":   header_rl["resetAt"]   or gql_rl.get("resetAt"),
                }

                # --- Primary rate limit épuisé sur 200 (doc: remaining header = 0) ---
                if rate_limit["remaining"] == 0:
                    wait = _wait_for_reset(rate_limit["resetAt"])
                    logger.warning(
                        "[%s] Primary rate limit épuisé (200) — attente %ds",
                        operation_name, wait,
                    )
                    time.sleep(wait)

                return result["data"], rate_limit

            except requests.exceptions.RequestException as exc:
                if attempt < _MAX_RETRIES:
                    wait = _backoff(attempt)
                    logger.warning(
                        "[%s] Erreur réseau : %s — retry %d/%d dans %ds",
                        operation_name,
                        exc,
                        attempt + 1,
                        _MAX_RETRIES,
                        wait,
                    )
                    time.sleep(wait)
                else:
                    raise RuntimeError(
                        f"[{operation_name}] Erreur réseau après "
                        f"{_MAX_RETRIES + 1} tentatives : {exc}"
                    ) from exc

        raise RuntimeError(f"[{operation_name}] Toutes les tentatives ont échoué")

    # ----------------------------------------------------------
    # Pagination générique — yield une liste de nodes par page
    # ----------------------------------------------------------

    def paginate(
        self,
        query: str,
        variables: dict[str, object],
        page_info_path: list[str],
        nodes_path: list[str],
        operation_name: str = "paginate",
    ) -> Generator[tuple[list, dict], None, None]:
        """
        Itère sur toutes les pages GraphQL.

        page_info_path : chemin dans data["x"]["y"]["pageInfo"]
        nodes_path     : chemin dans data["x"]["y"]["nodes"]
        Yield : (nodes, rate_limit_info) pour chaque page.
        """
        cursor = None

        while True:
            vars_with_cursor = {**variables, "after": cursor, "first": PAGE_SIZE}
            data, rate_limit = self.execute(query, vars_with_cursor, operation_name)

            # Naviguer jusqu'aux nodes et pageInfo
            nodes_container = data
            for key in nodes_path:
                nodes_container = nodes_container[key]

            page_info_container = data
            for key in page_info_path:
                page_info_container = page_info_container[key]

            nodes = nodes_container if isinstance(nodes_container, list) else []

            yield nodes, rate_limit

            if not page_info_container.get("hasNextPage"):
                break
            cursor = page_info_container["endCursor"]

    # ----------------------------------------------------------
    # REST API : récupère le README brut
    # ----------------------------------------------------------

    def fetch_readme_rest(self, owner: str, repo: str) -> str | None:
        """
        Récupère le README.md en utilisant l'API REST.
        Retourne le contenu du README ou None s'il n'existe pas.
        N'affecte que le rate limit REST (5000 req/h), pas les points GraphQL.
        """
        rest_headers = self._headers.copy()
        rest_headers["Accept"] = "application/vnd.github.v3.raw"

        url = f"https://api.github.com/repos/{owner}/{repo}/readme"

        for attempt in range(_MAX_RETRIES + 1):
            try:
                response = requests.get(
                    url,
                    headers=rest_headers,
                    timeout=30,
                )

                # README n'existe pas
                if response.status_code == 404:
                    logger.debug("[REST] README non trouvé : %s/%s", owner, repo)
                    return None

                # Erreurs serveur transitoires
                if response.status_code in _TRANSIENT_STATUS_CODES:
                    if attempt < _MAX_RETRIES:
                        wait = _backoff(attempt)
                        logger.warning(
                            "[REST] HTTP %s (%s/%s) — retry %d/%d dans %ds",
                            response.status_code, owner, repo,
                            attempt + 1, _MAX_RETRIES, wait,
                        )
                        time.sleep(wait)
                        continue
                    raise RuntimeError(
                        f"[REST] HTTP {response.status_code} ({owner}/{repo}) "
                        f"après {_MAX_RETRIES + 1} tentatives"
                    )

                if response.status_code == 403:
                    retry_after = response.headers.get("retry-after")
                    wait = int(retry_after) if retry_after else 60
                    if attempt < _MAX_RETRIES:
                        logger.warning(
                            "[REST] Rate limit (%s/%s) — attente %ds",
                            owner, repo, wait,
                        )
                        time.sleep(wait)
                        continue
                    raise RuntimeError(
                        f"[REST] Rate limit après {_MAX_RETRIES + 1} tentatives ({owner}/{repo})"
                    )

                if response.status_code != 200:
                    raise RuntimeError(
                        f"[REST] HTTP {response.status_code} ({owner}/{repo})\n{response.text}"
                    )

                logger.debug("[REST] README récupéré : %s/%s", owner, repo)
                return response.text

            except requests.exceptions.RequestException as exc:
                if attempt < _MAX_RETRIES:
                    wait = _backoff(attempt)
                    logger.warning(
                        "[REST] Erreur réseau (%s/%s) : %s — retry %d/%d dans %ds",
                        owner, repo, exc,
                        attempt + 1, _MAX_RETRIES, wait,
                    )
                    time.sleep(wait)
                else:
                    raise RuntimeError(
                        f"[REST] Erreur réseau après {_MAX_RETRIES + 1} tentatives ({owner}/{repo}) : {exc}"
                    ) from exc

        raise RuntimeError(f"[REST] Toutes les tentatives ont échoué ({owner}/{repo})")
