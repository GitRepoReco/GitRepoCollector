import logging

import psycopg2
from psycopg2 import errors

from database.connection import get_connection

logger = logging.getLogger(__name__)

REQUIRED_SCHEMAS = ("github", "monitoring")
REQUIRED_TABLES = (
    "github.repositories",
    "github.readmes",
    "github.repository_snapshots",
    "github.collection_progress",
    "monitoring.rate_limit_usage",
)
TABLE_REQUIRED_PRIVILEGES = {
    "github.repositories": ("SELECT", "INSERT", "UPDATE"),
    "github.readmes": ("SELECT", "INSERT"),
    "github.repository_snapshots": ("SELECT", "INSERT"),
    "github.collection_progress": ("SELECT", "INSERT", "UPDATE"),
    "monitoring.rate_limit_usage": ("SELECT", "INSERT"),
}
TABLE_SEQUENCE_COLUMNS = {
    "github.readmes": "id",
    "github.repository_snapshots": "id",
    "github.collection_progress": "id",
    "monitoring.rate_limit_usage": "id",
}


def _fetch_scalar(cur):
    row = cur.fetchone()
    if row is None:
        return None
    return row[0]


def _apply_schema_bootstrap(cur) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS github;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS monitoring;")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS github.repositories (
            id                   VARCHAR(100) PRIMARY KEY,
            name                 VARCHAR(255) NOT NULL,
            full_name            VARCHAR(255) UNIQUE NOT NULL,
            description          TEXT,
            url                  TEXT,
            homepage_url         TEXT,
            open_graph_image_url TEXT,
            stars                INTEGER NOT NULL DEFAULT 0,
            forks                INTEGER NOT NULL DEFAULT 0,
            disk_usage_kb        INTEGER,
            visibility           VARCHAR(20),
            is_archived          BOOLEAN DEFAULT FALSE,
            is_fork              BOOLEAN DEFAULT FALSE,
            is_template          BOOLEAN DEFAULT FALSE,
            is_disabled          BOOLEAN DEFAULT FALSE,
            is_mirror            BOOLEAN DEFAULT FALSE,
            mirror_url           TEXT,
            has_issues           BOOLEAN,
            has_wiki             BOOLEAN,
            has_discussions      BOOLEAN,
            merge_commit_allowed   BOOLEAN,
            squash_merge_allowed   BOOLEAN,
            rebase_merge_allowed   BOOLEAN,
            delete_branch_on_merge BOOLEAN,
            language             VARCHAR(100),
            language_color       VARCHAR(10),
            default_branch       VARCHAR(100),
            owner_login          VARCHAR(255),
            owner_avatar_url     TEXT,
            license_spdx_id      VARCHAR(50),
            license_name         VARCHAR(255),
            ssh_url              TEXT,
            parent_full_name     VARCHAR(255),
            parent_url           TEXT,
            code_of_conduct_name VARCHAR(255),
            code_of_conduct_url  TEXT,
            topics               TEXT[],
            languages            JSONB,
            total_releases       INTEGER,
            total_issues_open    INTEGER,
            total_issues_closed  INTEGER,
            total_prs_open       INTEGER,
            total_prs_merged     INTEGER,
            total_watchers       INTEGER,
            created_at           TIMESTAMP,
            updated_at           TIMESTAMP,
            pushed_at            TIMESTAMP,
            last_seen_commit_sha VARCHAR(100),
            last_commit_date     TIMESTAMP,
            last_checked_at      TIMESTAMP,
            collected_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_repositories_stars ON github.repositories(stars);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_repositories_language ON github.repositories(language);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_repositories_updated_at ON github.repositories(updated_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_repositories_last_checked ON github.repositories(last_checked_at);")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS github.readmes (
            id              BIGSERIAL PRIMARY KEY,
            repository_id   VARCHAR(100) NOT NULL
                                REFERENCES github.repositories(id),
            commit_sha      VARCHAR(100) NOT NULL,
            content         TEXT,
            collected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (repository_id, commit_sha)
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_readmes_repository_id ON github.readmes(repository_id);")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS github.repository_snapshots (
            id                  BIGSERIAL PRIMARY KEY,
            repository_id       VARCHAR(100) NOT NULL
                                    REFERENCES github.repositories(id),
            commit_sha          VARCHAR(100),
            snapshot_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            stars               INTEGER,
            forks               INTEGER,
            watchers            INTEGER,
            total_issues_open   INTEGER,
            total_issues_closed INTEGER,
            total_prs_open      INTEGER,
            total_prs_merged    INTEGER,
            total_releases      INTEGER,
            description         TEXT,
            language            VARCHAR(100),
            topics              TEXT[],
            is_archived         BOOLEAN,
            homepage_url        TEXT,
            disk_usage_kb       INTEGER
        );
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_snapshots_repository_at
            ON github.repository_snapshots(repository_id, snapshot_at);
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS monitoring.rate_limit_usage (
            id              BIGSERIAL PRIMARY KEY,
            timestamp       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            limit_value     INTEGER,
            remaining       INTEGER,
            used            INTEGER,
            cost            INTEGER,
            reset_at        TIMESTAMP,
            operation_name  VARCHAR(255)
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rate_limit_timestamp ON monitoring.rate_limit_usage(timestamp);")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS github.collection_progress (
            id               BIGSERIAL PRIMARY KEY,
            phase            VARCHAR(100) NOT NULL,
            segment_key      TEXT,
            tier_low         INTEGER NOT NULL,
            tier_high        INTEGER,
            date_low         DATE,
            date_high        DATE,
            sort_order       VARCHAR(50) NOT NULL,
            search_query     TEXT NOT NULL,
            cursor           TEXT,
            completed        BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (phase, tier_low, tier_high, sort_order)
        );
        """
    )

    cur.execute("ALTER TABLE github.collection_progress ADD COLUMN IF NOT EXISTS segment_key TEXT;")
    cur.execute("ALTER TABLE github.collection_progress ADD COLUMN IF NOT EXISTS date_low DATE;")
    cur.execute("ALTER TABLE github.collection_progress ADD COLUMN IF NOT EXISTS date_high DATE;")

    cur.execute(
        """
        UPDATE github.collection_progress
        SET date_low = COALESCE(date_low, DATE '2008-01-01'),
            date_high = COALESCE(date_high, DATE '2030-01-01')
        WHERE date_low IS NULL OR date_high IS NULL
        """
    )

    cur.execute(
        """
        UPDATE github.collection_progress
        SET segment_key = CONCAT(
            'stars_',
            tier_low,
            '_',
            COALESCE(tier_high::TEXT, 'inf'),
            '__pushed_',
            date_low::TEXT,
            '_',
            date_high::TEXT
        )
        WHERE segment_key IS NULL
        """
    )

    cur.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'collection_progress_phase_tier_low_tier_high_sort_order_key'
                  AND conrelid = 'github.collection_progress'::regclass
            ) THEN
                ALTER TABLE github.collection_progress
                DROP CONSTRAINT collection_progress_phase_tier_low_tier_high_sort_order_key;
            END IF;
        END $$;
        """
    )

    cur.execute(
        """
        WITH ranked AS (
            SELECT
                ctid,
                ROW_NUMBER() OVER (
                    PARTITION BY phase, segment_key, sort_order
                    ORDER BY updated_at DESC, id DESC
                ) AS rn
            FROM github.collection_progress
        )
        DELETE FROM github.collection_progress p
        USING ranked r
        WHERE p.ctid = r.ctid
          AND r.rn > 1
        """
    )

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_collection_progress_segment_unique
            ON github.collection_progress(phase, segment_key, sort_order);
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_collection_progress_lookup
            ON github.collection_progress(phase, tier_low, tier_high, completed);
        """
    )


def _verify_required_objects(conn: psycopg2.extensions.connection) -> None:
    missing_schemas: list[str] = []
    missing_tables: list[str] = []
    missing_schema_usage: list[str] = []
    missing_table_privileges: list[str] = []
    missing_sequence_privileges: list[str] = []

    with conn.cursor() as cur:
        for schema_name in REQUIRED_SCHEMAS:
            cur.execute("SELECT to_regnamespace(%s);", (schema_name,))
            if _fetch_scalar(cur) is None:
                missing_schemas.append(schema_name)
                continue
            cur.execute(
                "SELECT has_schema_privilege(current_user, %s, 'USAGE');",
                (schema_name,),
            )
            if not bool(_fetch_scalar(cur)):
                missing_schema_usage.append(schema_name)

        for table_name in REQUIRED_TABLES:
            cur.execute("SELECT to_regclass(%s);", (table_name,))
            if _fetch_scalar(cur) is None:
                missing_tables.append(table_name)
                continue

            for privilege in TABLE_REQUIRED_PRIVILEGES[table_name]:
                cur.execute(
                    "SELECT has_table_privilege(current_user, %s, %s);",
                    (table_name, privilege),
                )
                if not bool(_fetch_scalar(cur)):
                    missing_table_privileges.append(f"{table_name}:{privilege}")

        for table_name, id_column in TABLE_SEQUENCE_COLUMNS.items():
            if table_name in missing_tables:
                continue
            cur.execute("SELECT pg_get_serial_sequence(%s, %s);", (table_name, id_column))
            sequence_name = _fetch_scalar(cur)
            if sequence_name is None:
                continue
            cur.execute(
                "SELECT has_sequence_privilege(current_user, %s, 'USAGE');",
                (sequence_name,),
            )
            if not bool(_fetch_scalar(cur)):
                missing_sequence_privileges.append(f"{table_name}:{sequence_name}")

    if (
        missing_schemas
        or missing_tables
        or missing_schema_usage
        or missing_table_privileges
        or missing_sequence_privileges
    ):
        details: list[str] = []
        if missing_schemas:
            details.append(f"schémas manquants={', '.join(missing_schemas)}")
        if missing_tables:
            details.append(f"tables manquantes={', '.join(missing_tables)}")
        if missing_schema_usage:
            details.append(f"USAGE schémas manquant={', '.join(missing_schema_usage)}")
        if missing_table_privileges:
            details.append(
                "droits tables manquants=" + ", ".join(missing_table_privileges)
            )
        if missing_sequence_privileges:
            details.append(
                "droits séquences manquants=" + ", ".join(missing_sequence_privileges)
            )
        raise RuntimeError(
            "La base n'est pas prête pour un mode restreint collector: " + "; ".join(details)
        )


def initialize_schemas() -> None:
    try:
        conn = get_connection()
    except psycopg2.Error:
        logger.exception("Impossible de se connecter à PostgreSQL pour initialiser la BDD.")
        raise

    try:
        try:
            _verify_required_objects(conn)
            logger.info(
                "Base déjà initialisée et privilèges runtime présents: "
                "bootstrap DDL non nécessaire."
            )
            return
        except RuntimeError as verify_exc:
            logger.info(
                "Vérification pré-bootstrap non satisfaisante (%s). Tentative bootstrap DDL...",
                verify_exc,
            )

        with conn.cursor() as cur:
            _apply_schema_bootstrap(cur)
        conn.commit()
        _verify_required_objects(conn)
        logger.info(
            "Bootstrap DDL terminé et vérification runtime confirmée "
            "(schemas/tables/privilèges)."
        )
        return

    except errors.InsufficientPrivilege:
        conn.rollback()
        raise RuntimeError(
            "Droits DDL insuffisants et base runtime non prête. "
            "Initialiser la base avec un compte admin ou accorder les droits requis."
        ) from None

    except psycopg2.Error:
        conn.rollback()
        logger.exception("Erreur PostgreSQL lors de l'initialisation de la BDD.")
        raise

    finally:
        conn.close()
