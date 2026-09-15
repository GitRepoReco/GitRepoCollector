import logging

from database.connection import get_connection

logger = logging.getLogger(__name__)


def initialize_schemas() -> None:
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS github;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS monitoring;")

        # --------------------------------------------------------
        # github.repositories — état courant de chaque repo
        # --------------------------------------------------------
        cur.execute("""
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
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_repositories_stars ON github.repositories(stars);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_repositories_language ON github.repositories(language);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_repositories_updated_at ON github.repositories(updated_at);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_repositories_last_checked ON github.repositories(last_checked_at);")

        # --------------------------------------------------------
        # github.readmes — historique des README par commit
        # --------------------------------------------------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS github.readmes (
                id              BIGSERIAL PRIMARY KEY,
                repository_id   VARCHAR(100) NOT NULL
                                    REFERENCES github.repositories(id),
                commit_sha      VARCHAR(100) NOT NULL,
                content         TEXT,
                collected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (repository_id, commit_sha)
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_readmes_repository_id ON github.readmes(repository_id);")

        # --------------------------------------------------------
        # github.repository_snapshots — évolution dans le temps
        # --------------------------------------------------------
        cur.execute("""
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
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshots_repository_at
                ON github.repository_snapshots(repository_id, snapshot_at);
        """)

        # --------------------------------------------------------
        # monitoring.rate_limit_usage
        # --------------------------------------------------------
        cur.execute("""
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
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rate_limit_timestamp ON monitoring.rate_limit_usage(timestamp);")

        # --------------------------------------------------------
        # github.collection_progress — reprise pagination historique
        # --------------------------------------------------------
        cur.execute("""
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
        """)

        # Migration: ajouter les colonnes utiles à la segmentation dynamique.
        cur.execute("ALTER TABLE github.collection_progress ADD COLUMN IF NOT EXISTS segment_key TEXT;")
        cur.execute("ALTER TABLE github.collection_progress ADD COLUMN IF NOT EXISTS date_low DATE;")
        cur.execute("ALTER TABLE github.collection_progress ADD COLUMN IF NOT EXISTS date_high DATE;")

        # Migration: renseigner les segments historiques avec des bornes de date par défaut.
        cur.execute("""
            UPDATE github.collection_progress
            SET date_low = COALESCE(date_low, DATE '2008-01-01'),
                date_high = COALESCE(date_high, DATE '2030-01-01')
            WHERE date_low IS NULL OR date_high IS NULL
        """)

        cur.execute("""
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
        """)

        # Migration: remplacer l'unicité historique par une unicité par segment_key.
        cur.execute("""
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
        """)

        # Migration: supprimer les doublons historiques (notamment liés à tier_high NULL)
        # avant de créer l'unicité par segment. On conserve la ligne la plus récente.
        cur.execute("""
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
        """)

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

        conn.commit()
        logger.info("Schemas et tables initialisés.")

    finally:
        cur.close()
        conn.close()

