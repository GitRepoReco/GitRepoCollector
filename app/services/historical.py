import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

from config import MAX_REPOSITORIES, STAR_TIERS
from database.connection import get_connection
from github.client import GitHubGraphQLClient
from github.queries import (
    QUERY_REPOSITORY_README,
    QUERY_REPOSITORY_STATS,
)
from monitoring.rate_limit import RateLimitMonitor

logger = logging.getLogger(__name__)

DBConn = Any


# ============================================================
# Construction des tranches de stars depuis les tiers
# ============================================================

def _build_tier_ranges(tiers: list[int]) -> list[tuple[int, int | None]]:
    """
    Exemple : [100000, 75000, 50000, 1000]
    → [(100000, None), (75000, 99999), (50000, 74999), (1000, 49999)]
    """
    ranges = []
    for i, tier in enumerate(tiers):
        high = tiers[i - 1] - 1 if i > 0 else None
        ranges.append((tier, high))
    return ranges


def _search_query_for_tier(low: int, high: int | None) -> str:
    if high is None:
        return f"stars:>={low}"
    return f"stars:{low}..{high}"

def _parse_repo(node: dict) -> dict:
    lang = node.get("primaryLanguage") or {}
    branch_ref = node.get("defaultBranchRef") or {}
    owner = node.get("owner") or {}
    license_info = node.get("licenseInfo") or {}

    return {
        "id":                     node["id"],
        "name":                   node["name"],
        "full_name":              node["nameWithOwner"],
        "description":            node.get("description"),
        "url":                    node.get("url"),
        "ssh_url":                node.get("sshUrl"),
        "homepage_url":           node.get("homepageUrl"),
        "open_graph_image_url":   node.get("openGraphImageUrl"),
        "stars":                  node.get("stargazerCount", 0),
        "forks":                  node.get("forkCount", 0),
        "disk_usage_kb":          node.get("diskUsage"),
        "visibility":             node.get("visibility"),
        "is_archived":            node.get("isArchived", False),
        "is_fork":                node.get("isFork", False),
        "is_template":            node.get("isTemplate", False),
        "is_disabled":            node.get("isDisabled", False),
        "is_mirror":              node.get("isMirror", False),
        "mirror_url":             node.get("mirrorUrl"),
        "has_issues":             node.get("hasIssuesEnabled"),
        "has_wiki":               node.get("hasWikiEnabled"),
        "has_discussions":        node.get("hasDiscussionsEnabled"),
        "merge_commit_allowed":   node.get("mergeCommitAllowed"),
        "squash_merge_allowed":   node.get("squashMergeAllowed"),
        "rebase_merge_allowed":   node.get("rebaseMergeAllowed"),
        "delete_branch_on_merge": node.get("deleteBranchOnMerge"),
        "language":               lang.get("name"),
        "language_color":         lang.get("color"),
        "default_branch":         branch_ref.get("name"),
        "owner_login":            owner.get("login"),
        "owner_avatar_url":       owner.get("avatarUrl"),
        "license_spdx_id":        license_info.get("spdxId"),
        "license_name":           license_info.get("name"),
        "parent_full_name":       (node.get("parent") or {}).get("nameWithOwner"),
        "parent_url":             (node.get("parent") or {}).get("url"),
        "code_of_conduct_name":   (node.get("codeOfConduct") or {}).get("name"),
        "code_of_conduct_url":    (node.get("codeOfConduct") or {}).get("url"),
        "created_at":             node.get("createdAt"),
        "updated_at":             node.get("updatedAt"),
        "pushed_at":              node.get("pushedAt"),
    }


# ============================================================
# Persistance des repos (metadata uniquement)
# ============================================================

def _upsert_repositories(conn, repos: list[dict]) -> None:
    if not repos:
        return
    cur = conn.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO github.repositories (
                id, name, full_name, description, url, ssh_url,
                homepage_url, open_graph_image_url,
                stars, forks, disk_usage_kb, visibility,
                is_archived, is_fork, is_template, is_disabled, is_mirror, mirror_url,
                has_issues, has_wiki, has_discussions,
                merge_commit_allowed, squash_merge_allowed, rebase_merge_allowed, delete_branch_on_merge,
                language, language_color,
                default_branch, owner_login, owner_avatar_url,
                license_spdx_id, license_name,
                parent_full_name, parent_url,
                code_of_conduct_name, code_of_conduct_url,
                created_at, updated_at, pushed_at
            ) VALUES (
                %(id)s, %(name)s, %(full_name)s, %(description)s, %(url)s, %(ssh_url)s,
                %(homepage_url)s, %(open_graph_image_url)s,
                %(stars)s, %(forks)s, %(disk_usage_kb)s, %(visibility)s,
                %(is_archived)s, %(is_fork)s, %(is_template)s, %(is_disabled)s, %(is_mirror)s, %(mirror_url)s,
                %(has_issues)s, %(has_wiki)s, %(has_discussions)s,
                %(merge_commit_allowed)s, %(squash_merge_allowed)s, %(rebase_merge_allowed)s, %(delete_branch_on_merge)s,
                %(language)s, %(language_color)s,
                %(default_branch)s, %(owner_login)s, %(owner_avatar_url)s,
                %(license_spdx_id)s, %(license_name)s,
                %(parent_full_name)s, %(parent_url)s,
                %(code_of_conduct_name)s, %(code_of_conduct_url)s,
                %(created_at)s, %(updated_at)s, %(pushed_at)s
            )
            ON CONFLICT (id) DO UPDATE SET
                description            = EXCLUDED.description,
                url                    = EXCLUDED.url,
                ssh_url                = EXCLUDED.ssh_url,
                homepage_url           = EXCLUDED.homepage_url,
                open_graph_image_url   = EXCLUDED.open_graph_image_url,
                stars                  = EXCLUDED.stars,
                forks                  = EXCLUDED.forks,
                disk_usage_kb          = EXCLUDED.disk_usage_kb,
                visibility             = EXCLUDED.visibility,
                is_archived            = EXCLUDED.is_archived,
                is_disabled            = EXCLUDED.is_disabled,
                is_mirror              = EXCLUDED.is_mirror,
                mirror_url             = EXCLUDED.mirror_url,
                has_issues             = EXCLUDED.has_issues,
                has_wiki               = EXCLUDED.has_wiki,
                has_discussions        = EXCLUDED.has_discussions,
                merge_commit_allowed   = EXCLUDED.merge_commit_allowed,
                squash_merge_allowed   = EXCLUDED.squash_merge_allowed,
                rebase_merge_allowed   = EXCLUDED.rebase_merge_allowed,
                delete_branch_on_merge = EXCLUDED.delete_branch_on_merge,
                language               = EXCLUDED.language,
                language_color         = EXCLUDED.language_color,
                default_branch         = EXCLUDED.default_branch,
                license_spdx_id        = EXCLUDED.license_spdx_id,
                license_name           = EXCLUDED.license_name,
                parent_full_name       = EXCLUDED.parent_full_name,
                parent_url             = EXCLUDED.parent_url,
                updated_at             = EXCLUDED.updated_at,
                pushed_at              = EXCLUDED.pushed_at,
                collected_at           = CURRENT_TIMESTAMP
            """,
            repos,
        )
        conn.commit()
    finally:
        cur.close()


# ============================================================
# Suivi du commit HEAD
# ============================================================

def _update_repo_commit_state(conn: DBConn, repository_id: str, sha: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE github.repositories
            SET last_seen_commit_sha = %s,
                last_checked_at      = CURRENT_TIMESTAMP
            WHERE id = %s
            """,
            (sha, repository_id),
        )
        conn.commit()
    finally:
        cur.close()


# ============================================================
# README, stats, topics, languages
# ============================================================

def _upsert_readme(conn: DBConn, repository_id: str, commit_sha: str, content: str | None) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO github.readmes (repository_id, commit_sha, content)
            VALUES (%s, %s, %s)
            ON CONFLICT (repository_id, commit_sha) DO NOTHING
            """,
            (repository_id, commit_sha, content),
        )
        conn.commit()
    finally:
        cur.close()


def _update_repo_readme_stats(conn: DBConn, repository_id: str, stats: dict[str, Any]) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE github.repositories SET
                total_releases      = %s,
                total_issues_open   = %s,
                total_issues_closed = %s,
                total_prs_open      = %s,
                total_prs_merged    = %s,
                total_watchers      = %s,
                topics              = %s,
                languages           = %s::jsonb
            WHERE id = %s
            """,
            (
                stats.get("total_releases"),
                stats.get("total_issues_open"),
                stats.get("total_issues_closed"),
                stats.get("total_prs_open"),
                stats.get("total_prs_merged"),
                stats.get("total_watchers"),
                stats.get("topics") or [],
                json.dumps(stats.get("languages") or []),
                repository_id,
            ),
        )
        conn.commit()
    finally:
        cur.close()


def _readme_already_collected(conn: DBConn, repository_id: str) -> bool:
    """Retourne True si au moins un README existe pour ce repo (passe déjà effectuée)."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM github.readmes WHERE repository_id = %s)",
            (repository_id,),
        )
        return cur.fetchone()[0]
    finally:
        cur.close()


def collect_readme_for_repo(
    conn: DBConn,
    repo: dict[str, Any],
    client: GitHubGraphQLClient,
    monitor: RateLimitMonitor,
) -> str | None:
    """
    Récupère le README (REST GET parallélisé) et les stats (GraphQL).
    Retourne le commit_sha courant ou None.
    
    Stratégie hybride pour optimiser les requêtes :
    - README : API REST (1 req, utilise rate limit REST uniquement)
    - Stats : GraphQL (1 point, utilise rate limit GraphQL)
    """
    owner, name = repo["full_name"].split("/", 1)

    # Paralléliser : README en REST + Stats en GraphQL
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Lancer les deux requêtes en parallèle
        readme_future = executor.submit(client.fetch_readme_rest, owner, name)
        stats_future = executor.submit(
            client.execute,
            QUERY_REPOSITORY_STATS,
            {"owner": owner, "name": name},
            "RepositoryStats",
        )

        # Récupérer le README (n'attend que le réseau)
        readme_content = readme_future.result()

        # Récupérer les stats (GraphQL)
        data, rate_limit = stats_future.result()
        monitor.record(conn, rate_limit, "RepositoryStats")
        monitor.check_and_wait(rate_limit)

    repo_data = data.get("repository") or {}
    if not repo_data:
        return None

    branch_ref: dict = repo_data.get("defaultBranchRef") or {}
    target: dict = branch_ref.get("target") or {}
    commit_sha: str | None = target.get("oid")

    stats = {
        "total_releases":      (repo_data.get("releases") or {}).get("totalCount"),
        "total_issues_open":   (repo_data.get("openIssues") or {}).get("totalCount"),
        "total_issues_closed": (repo_data.get("closedIssues") or {}).get("totalCount"),
        "total_prs_open":      (repo_data.get("openPRs") or {}).get("totalCount"),
        "total_prs_merged":    (repo_data.get("mergedPRs") or {}).get("totalCount"),
        "total_watchers":      (repo_data.get("watchers") or {}).get("totalCount"),
        "topics": [
            t["topic"]["name"]
            for t in ((repo_data.get("repositoryTopics") or {}).get("nodes") or [])
            if t.get("topic")
        ],
        "languages": [
            {"name": l["name"], "color": l.get("color")}
            for l in ((repo_data.get("languages") or {}).get("nodes") or [])
        ],
    }

    if commit_sha:
        _upsert_readme(conn, repo["id"], commit_sha, readme_content)

    _update_repo_readme_stats(conn, repo["id"], stats)
    return commit_sha


# ============================================================
# Snapshot initial (après la passe README)
# ============================================================

def _create_snapshot_if_missing(conn: DBConn, repo_id: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM github.repository_snapshots WHERE repository_id = %s)",
            (repo_id,),
        )
        if cur.fetchone()[0]:
            return

        cur.execute(
            """
            INSERT INTO github.repository_snapshots (
                repository_id, commit_sha, stars, forks, watchers,
                total_issues_open, total_issues_closed,
                total_prs_open, total_prs_merged, total_releases,
                description, language, topics, is_archived, homepage_url, disk_usage_kb
            )
            SELECT
                id, last_seen_commit_sha, stars, forks, total_watchers,
                total_issues_open, total_issues_closed,
                total_prs_open, total_prs_merged, total_releases,
                description, language, topics, is_archived, homepage_url, disk_usage_kb
            FROM github.repositories
            WHERE id = %s
            """,
            (repo_id,),
        )
        conn.commit()
    finally:
        cur.close()


# ============================================================
# Collecte principale
# ============================================================

class HistoricalCollector:

    def __init__(self) -> None:
        self._client = GitHubGraphQLClient()
        self._monitor = RateLimitMonitor()

    def collect_all(self) -> None:
        logger.info("=== DÉBUT DE LA COLLECTE HISTORIQUE ===")
        logger.info("STAR_TIERS=%s  MAX_REPOSITORIES=%d", STAR_TIERS, MAX_REPOSITORIES)

        tier_ranges = _build_tier_ranges(STAR_TIERS)
        total_repos = 0

        for low, high in tier_ranges:
            if MAX_REPOSITORIES and total_repos >= MAX_REPOSITORIES:
                logger.info("Limite MAX_REPOSITORIES=%d atteinte.", MAX_REPOSITORIES)
                break

            label = _search_query_for_tier(low, high)
            logger.info("=== PASSE : %s ===", label)

            try:
                # Phase 1 : metadata
                repos_in_tier = self._collect_tier_metadata(low, high, total_repos)
                total_repos += len(repos_in_tier)

                # Phase 2 : README, stats, topics, languages
                self._collect_tier_readmes(repos_in_tier)

            except Exception:
                logger.exception("Erreur sur la passe %s — passage à la suivante.", label)

        logger.info("=== COLLECTE HISTORIQUE TERMINÉE — %d repositories ===", total_repos)

    # ----------------------------------------------------------
    # Phase 1 : métadonnées pour une tranche
    # ----------------------------------------------------------

    def _collect_tier_metadata(
        self, low: int, high: int | None, already_collected: int
    ) -> list[dict]:
        """Collecte les métadonnées d'une tranche en utilisant la pagination cursor GitHub."""
        logger.info("  Collecte des métadonnées via pagination cursor GitHub...")

        conn = get_connection()
        try:
            search_query = _search_query_for_tier(low, high)
            collected: list[dict] = []

            for nodes, rate_limit in self._client.paginate_searches(
                search_query,
                ["created-asc", "updated-desc"],
                operation_name="SearchRepositories",
            ):
                self._monitor.record(conn, rate_limit, "SearchRepositories")
                self._monitor.check_and_wait(rate_limit)

                for node in nodes:
                    if MAX_REPOSITORIES and already_collected + len(collected) >= MAX_REPOSITORIES:
                        break
                    repo = _parse_repo(node)
                    if repo.get("id"):
                        collected.append(repo)

                if MAX_REPOSITORIES and already_collected + len(collected) >= MAX_REPOSITORIES:
                    break

            _upsert_repositories(conn, collected)

            logger.info("  Métadonnées collectées : %d repositories (cette tranche).", len(collected))

        finally:
            conn.close()

        return collected

    # ----------------------------------------------------------
    # Phase 2 : README + stats pour tous les repos de la tranche
    # ----------------------------------------------------------

    def _collect_tier_readmes(self, repos: list[dict]) -> None:
        """
        Collecte READMEs et stats pour tous les repos en parallèle.
        Utilise ThreadPoolExecutor pour paralléliser les requêtes REST (README)
        et GraphQL (stats) pour chaque repo.
        """
        logger.info("  Passe README : %d repositories à traiter.", len(repos))

        # Filtrer les repos déjà collectés
        conn_check = get_connection()
        try:
            repos_to_collect = []
            for repo in repos:
                if _readme_already_collected(conn_check, repo["id"]):
                    logger.debug("  [%s] README déjà collecté, skip.", repo["full_name"])
                else:
                    repos_to_collect.append(repo)
        finally:
            conn_check.close()

        logger.info("  %d repositories à traiter (après filtrage).", len(repos_to_collect))

        # Paralléliser : max 5 workers pour ne pas surcharger l'API
        max_workers = min(5, len(repos_to_collect))

        def _collect_single_repo(repo: dict) -> tuple[str, str | None, Exception | None]:
            """Wrapper pour collecte d'un seul repo. Retourne (full_name, commit_sha, error)."""
            conn = get_connection()
            try:
                commit_sha = collect_readme_for_repo(
                    conn, repo, self._client, self._monitor
                )
                if commit_sha:
                    _update_repo_commit_state(conn, repo["id"], commit_sha)
                _create_snapshot_if_missing(conn, repo["id"])
                logger.debug("  [%s] README collecté.", repo["full_name"])
                return (repo["full_name"], commit_sha, None)
            except Exception as e:
                logger.exception("  Erreur README pour %s — repo ignoré.", repo["full_name"])
                return (repo["full_name"], None, e)
            finally:
                conn.close()

        # Paralléliser les collectes
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_collect_single_repo, repo): repo["full_name"]
                for repo in repos_to_collect
            }

            completed = 0
            for future in as_completed(futures):
                completed += 1
                full_name, commit_sha, error = future.result()
                if error is None:
                    logger.debug(
                        "  [%d/%d] %s collecté.",
                        completed, len(repos_to_collect), full_name,
                    )
                else:
                    logger.debug(
                        "  [%d/%d] %s — erreur.",
                        completed, len(repos_to_collect), full_name,
                    )

        logger.info("  Passe README terminée (%d repos parallélisés).", len(repos_to_collect))


