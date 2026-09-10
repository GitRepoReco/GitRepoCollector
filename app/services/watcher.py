import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from config import STAR_TIERS, POLL_INTERVAL_SECONDS
from database.connection import get_connection
from github.client import GitHubGraphQLClient
from github.queries import QUERY_SEARCH_REPOSITORIES
from monitoring.rate_limit import RateLimitMonitor
from services.historical import (
    _parse_repo,
    _update_repo_commit_state,
    _upsert_repositories,
    collect_readme_for_repo,
)

logger = logging.getLogger(__name__)

DBConn = Any


# ============================================================
# Helpers DB
# ============================================================

def _get_cutoff(conn: DBConn) -> datetime | None:
    """Retourne le MIN(last_checked_at) — seuil en dessous duquel rien n'a changé."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT MIN(last_checked_at) FROM github.repositories WHERE last_checked_at IS NOT NULL"
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        # PostgreSQL retourne un datetime naive en UTC ; l'ajouter à tzinfo pour compatibilité
        dt = row[0]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    finally:
        cur.close()


def _get_stored_repos_by_ids(conn: DBConn, ids: list[str]) -> dict[str, dict]:
    """Charge les états stockés pour un lot d'IDs en une seule requête."""
    if not ids:
        return {}
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, full_name, default_branch, last_seen_commit_sha,
                   updated_at, stars, forks, description, language, is_archived,
                   total_watchers, total_issues_open, total_prs_open,
                   total_releases, homepage_url, disk_usage_kb, topics
            FROM github.repositories
            WHERE id = ANY(%s)
            """,
            (ids,),
        )
        cols = [d[0] for d in cur.description]
        return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}
    finally:
        cur.close()


def _mark_all_checked(conn: DBConn) -> None:
    cur = conn.cursor()
    try:
        cur.execute("UPDATE github.repositories SET last_checked_at = CURRENT_TIMESTAMP")
        conn.commit()
    finally:
        cur.close()


def _insert_snapshot(conn: DBConn, repo_id: str, commit_sha: str | None, repo_state: dict) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO github.repository_snapshots (
                repository_id, commit_sha, stars, forks, watchers,
                total_issues_open, total_issues_closed,
                total_prs_open, total_prs_merged, total_releases,
                description, language, topics, is_archived, homepage_url, disk_usage_kb
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                repo_id,
                commit_sha,
                repo_state.get("stars"),
                repo_state.get("forks"),
                repo_state.get("total_watchers"),
                repo_state.get("total_issues_open"),
                repo_state.get("total_issues_closed"),
                repo_state.get("total_prs_open"),
                repo_state.get("total_prs_merged"),
                repo_state.get("total_releases"),
                repo_state.get("description"),
                repo_state.get("language"),
                repo_state.get("topics") or [],
                repo_state.get("is_archived"),
                repo_state.get("homepage_url"),
                repo_state.get("disk_usage_kb"),
            ),
        )
        conn.commit()
    finally:
        cur.close()


# ============================================================
# Service de surveillance
# ============================================================

class WatcherService:

    def __init__(self) -> None:
        self._client = GitHubGraphQLClient()
        self._monitor = RateLimitMonitor()

    def run(self) -> None:
        logger.info("=== MODE WATCH ACTIF (intervalle : %ds) ===", POLL_INTERVAL_SECONDS)
        while True:
            try:
                self._watch_cycle()
            except Exception:
                logger.exception("Erreur lors du cycle de surveillance.")
            logger.info("Prochain cycle dans %ds.", POLL_INTERVAL_SECONDS)
            time.sleep(POLL_INTERVAL_SECONDS)

    # ----------------------------------------------------------
    # Cycle de surveillance : scan sort:updated-desc
    # ----------------------------------------------------------

    def _watch_cycle(self) -> None:
        conn = get_connection()
        try:
            cutoff = _get_cutoff(conn)
        finally:
            conn.close()

        if cutoff:
            logger.info(
                "Cycle de surveillance — cutoff : %s",
                cutoff.strftime("%Y-%m-%d %H:%M:%S"),
            )
        else:
            logger.info("Cycle de surveillance — premier passage (pas de cutoff).")

        updated_count = 0
        search_query = f"stars:>={STAR_TIERS[0]} sort:updated-desc"

        for nodes, rate_limit in self._client.paginate(
            QUERY_SEARCH_REPOSITORIES,
            {"query": search_query},
            page_info_path=["search", "pageInfo"],
            nodes_path=["search", "nodes"],
            operation_name="WatchSearch",
        ):
            conn = get_connection()
            try:
                self._monitor.record(conn, rate_limit, "WatchSearch")
                self._monitor.check_and_wait(rate_limit)

                ids = [n["id"] for n in nodes if n.get("id")]
                stored_map = _get_stored_repos_by_ids(conn, ids)

                stop_pagination = True  # devient False si au moins un repo est potentiellement nouveau

                for node in nodes:
                    if not node.get("id"):
                        continue

                    node_updated_at = _parse_dt(node.get("updatedAt"))

                    # Nouveau repo : onboarding complet
                    if node["id"] not in stored_map:
                        stop_pagination = False
                        logger.info("[%s] Nouveau repo détecté — onboarding.", node["nameWithOwner"])
                        self._onboard_new_repo(conn, node)
                        updated_count += 1
                        continue

                    stored = stored_map[node["id"]]
                    stored_updated_at = stored.get("updated_at")

                    # Si updatedAt antérieur au cutoff : aucun repo suivant ne peut être nouveau
                    if cutoff and node_updated_at and node_updated_at <= cutoff:
                        logger.info("Cutoff atteint — fin du cycle de surveillance.")
                        _mark_all_checked(conn)
                        return

                    stop_pagination = False

                    # Comparer avec l'état stocké
                    if stored_updated_at and node_updated_at and node_updated_at <= stored_updated_at:
                        continue  # ce repo spécifique n'a pas changé

                    logger.info("[%s] Changement détecté.", node["nameWithOwner"])
                    self._process_changed_repo(conn, node, stored)
                    updated_count += 1

            finally:
                conn.close()

            if stop_pagination:
                break

        conn = get_connection()
        try:
            _mark_all_checked(conn)
        finally:
            conn.close()

        logger.info("Cycle terminé — %d repositories mis à jour.", updated_count)

    # ----------------------------------------------------------
    # Onboarding complet d'un repo inconnu qui atteint le seuil
    # ----------------------------------------------------------

    def _onboard_new_repo(self, conn: DBConn, node: dict) -> None:
        repo = _parse_repo(node)
        _upsert_repositories(conn, [repo])

        # README + stats + snapshot initial
        commit_sha = collect_readme_for_repo(conn, repo, self._client, self._monitor)
        if commit_sha:
            _update_repo_commit_state(conn, repo["id"], commit_sha)

        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO github.repository_snapshots (
                    repository_id, commit_sha, stars, forks, watchers,
                    total_issues_open, total_issues_closed,
                    total_prs_open, total_prs_merged, total_releases,
                    description, language, topics, is_archived, homepage_url, disk_usage_kb
                )
                SELECT id, last_seen_commit_sha, stars, forks, total_watchers,
                       total_issues_open, total_issues_closed,
                       total_prs_open, total_prs_merged, total_releases,
                       description, language, topics, is_archived, homepage_url, disk_usage_kb
                FROM github.repositories WHERE id = %s
                """,
                (repo["id"],),
            )
            conn.commit()
        finally:
            cur.close()

    # ----------------------------------------------------------
    # Traitement d'un repo dont updatedAt a changé
    # ----------------------------------------------------------

    def _process_changed_repo(self, conn: DBConn, node: dict, stored: dict) -> None:
        repo = _parse_repo(node)
        _upsert_repositories(conn, [repo])

        commit_sha = collect_readme_for_repo(conn, repo, self._client, self._monitor)
        if commit_sha:
            _update_repo_commit_state(conn, repo["id"], commit_sha)

        # Snapshot avec toutes les métriques courantes
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT stars, forks, total_watchers, total_issues_open, total_issues_closed,
                       total_prs_open, total_prs_merged, total_releases,
                       description, language, topics, is_archived, homepage_url, disk_usage_kb
                FROM github.repositories WHERE id = %s
                """,
                (repo["id"],),
            )
            row = cur.fetchone()
        finally:
            cur.close()

        if row:
            cols = ["stars", "forks", "total_watchers", "total_issues_open", "total_issues_closed",
                    "total_prs_open", "total_prs_merged", "total_releases",
                    "description", "language", "topics", "is_archived", "homepage_url", "disk_usage_kb"]
            state = dict(zip(cols, row))
            _insert_snapshot(conn, repo["id"], commit_sha, state)

# ============================================================
# Utilitaire
# ============================================================

def _parse_dt(iso: str | None) -> datetime | None:
    if not iso:
        return None
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))
