"""
Récupération de repos GitHub via l'API GraphQL, avec filtrage sur la langue
naturelle (anglais) de la description / du README, en plus de critères
GitHub natifs (étoiles, dates, topics, licence...).

Objectif final : constituer un corpus de README exploitable pour du RAG
(recherche sémantique de projets à partir d'une requête en langage naturel).

Structure de sortie :
    repos.db          → base SQLite unique, une ligne par repo :
                         métadonnées + README (compressé en gzip dans la
                         colonne readme_compressed). Un seul fichier, pas de
                         problème de multiplication de petits fichiers.
    fetch_state.json  → progression de pagination par segment de recherche,
                         pour reprendre là où le run précédent s'est arrêté.

Étapes suivantes (plus tard, pas dans ce script) :
    - chunker les README (get_all_readmes() fournit déjà le texte décompressé)
    - générer des embeddings et les stocker (nouvelle colonne, ou un index
      vectoriel externe type FAISS/Chroma, avec l'id `name` comme clé de jointure)
    - requêter par similarité pour retrouver un projet à partir d'une description

Prérequis :
    pip install requests langdetect

Auth :
    Crée un Personal Access Token (classic ou fine-grained) sur GitHub :
    https://github.com/settings/tokens
    Scope nécessaire pour du contenu public : aucun scope obligatoire
    (public_repo suffit si tu veux aussi du contenu privé accessible).

Usage :
    export GITHUB_TOKEN="ghp_xxxxx"
    python github_repo_fetcher.py
"""

import os
import time
import json
import gzip
import sqlite3
import collections
import itertools
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, date, timedelta

import requests
from langdetect import detect, LangDetectException

MIN_README_LENGTH = 100  # caractères minimum pour garder le repo

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise SystemExit("Définis la variable d'environnement GITHUB_TOKEN avant de lancer le script.")

API_URL = "https://api.github.com/graphql"
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json",
}

DB_FILE = "repos.db"
STATE_FILE = "fetch_state.json"


class SlidingWindowRateLimiter:
    """
    Plafonne le nombre d'appels à `max_per_minute` sur une fenêtre glissante
    de 60 secondes, partagée entre tous les threads. Sert à respecter le
    rate limit SECONDAIRE de GitHub pour GraphQL (2000 points/minute réels ;
    on vise nettement en dessous par sécurité, notre coût observé étant
    ~1 point/requête, donc "appels" et "points" sont quasi équivalents ici).
    """
    def __init__(self, max_per_minute: int):
        self.max_per_minute = max_per_minute
        self._timestamps = collections.deque()
        self._lock = threading.Lock()

    def wait_if_needed(self):
        while True:
            with self._lock:
                now = time.time()
                while self._timestamps and now - self._timestamps[0] > 60:
                    self._timestamps.popleft()

                if len(self._timestamps) < self.max_per_minute:
                    self._timestamps.append(now)
                    return

                sleep_time = 60 - (now - self._timestamps[0]) + 0.1

            time.sleep(sleep_time)  # en dehors du verrou, pour ne pas bloquer les autres threads


# Marge de sécurité sous les 2000 points/minute documentés par GitHub pour
# l'API GraphQL (limite secondaire) : on vise 1500 pour absorber les pics.
GRAPHQL_RATE_LIMITER = SlidingWindowRateLimiter(max_per_minute=1500)

# ---------------------------------------------------------------------------
# 1. Requête GraphQL : recherche paginée par curseur
# ---------------------------------------------------------------------------
SEARCH_QUERY = """
query ($searchQuery: String!, $cursor: String) {
  search(query: $searchQuery, type: REPOSITORY, first: 50, after: $cursor) {
    repositoryCount
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ... on Repository {
        nameWithOwner
        url
        description
        stargazerCount
        forkCount
        primaryLanguage { name }
        licenseInfo { spdxId }
        createdAt
        pushedAt
        isArchived
        repositoryTopics(first: 10) {
          nodes { topic { name } }
        }
        readmeMd: object(expression: "HEAD:README.md") {
          ... on Blob { text }
        }
        readmeLower: object(expression: "HEAD:readme.md") {
          ... on Blob { text }
        }
        readmeRst: object(expression: "HEAD:README.rst") {
          ... on Blob { text }
        }
        readmeNoExt: object(expression: "HEAD:README") {
          ... on Blob { text }
        }
      }
    }
  }
  rateLimit {
    remaining
    resetAt
  }
}
"""


def run_query(search_query: str, cursor: str = None, max_retries: int = 5) -> dict:
    payload = {
        "query": SEARCH_QUERY,
        "variables": {"searchQuery": search_query, "cursor": cursor},
    }

    for attempt in range(1, max_retries + 1):
        try:
            GRAPHQL_RATE_LIMITER.wait_if_needed()
            resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)

            if resp.status_code in (401, 403):
                # Rate limit SECONDAIRE (anti-abus GitHub, distinct du budget de
                # 5000 points/heure) : ce n'est pas une erreur de token, il faut
                # attendre puis réessayer, en suivant les règles officielles :
                # 1. Respecter Retry-After s'il est présent
                # 2. Sinon, si X-RateLimit-Remaining vaut 0, attendre jusqu'à X-RateLimit-Reset
                # 3. Sinon, attendre au moins 60s, avec backoff croissant sur les tentatives répétées
                is_secondary_limit = "secondary rate limit" in resp.text.lower()

                if is_secondary_limit:
                    retry_after = resp.headers.get("Retry-After")
                    remaining = resp.headers.get("X-RateLimit-Remaining")
                    reset_ts = resp.headers.get("X-RateLimit-Reset")

                    if retry_after:
                        wait = int(retry_after) + 5
                    elif remaining == "0" and reset_ts:
                        wait = max(int(reset_ts) - int(time.time()), 60) + 5
                    else:
                        wait = 60 * attempt  # backoff croissant : 60s, 120s, 180s...

                    print(f"Rate limit secondaire atteint (tentative {attempt}/{max_retries}), "
                          f"pause de {wait}s avant nouvelle tentative...")
                    time.sleep(wait)
                    continue

                # 401/403 "normal" (token invalide/expiré/permissions insuffisantes)
                # : inutile de réessayer, on échoue tout de suite.
                raise RuntimeError(
                    f"Erreur {resp.status_code} : vérifie ton GITHUB_TOKEN et ses permissions. "
                    f"Réponse : {resp.text[:300]}"
                )

            if resp.status_code in (502, 503, 504):
                wait = 2 ** attempt
                print(f"Erreur {resp.status_code} (tentative {attempt}/{max_retries}), "
                      f"nouvelle tentative dans {wait}s...")
                time.sleep(wait)
                continue

            resp.raise_for_status()

            try:
                data = resp.json()
            except requests.exceptions.JSONDecodeError:
                wait = 2 ** attempt
                print(f"Réponse non-JSON reçue (statut {resp.status_code}, corps: "
                      f"{resp.text[:150]!r}) — tentative {attempt}/{max_retries}, "
                      f"nouvelle tentative dans {wait}s...")
                time.sleep(wait)
                continue

            if "errors" in data:
                raise RuntimeError(data["errors"])
            return data["data"]

        except requests.exceptions.Timeout:
            wait = 2 ** attempt
            print(f"Timeout (tentative {attempt}/{max_retries}), nouvelle tentative dans {wait}s...")
            time.sleep(wait)
        except requests.exceptions.ConnectionError:
            wait = 2 ** attempt
            print(f"Erreur de connexion (tentative {attempt}/{max_retries}), "
                  f"nouvelle tentative dans {wait}s...")
            time.sleep(wait)
        except requests.exceptions.RequestException as e:
            # Filet de sécurité pour tout autre incident réseau transitoire
            # (connexion coupée en cours de réponse, etc.) non couvert ci-dessus.
            wait = 2 ** attempt
            print(f"Erreur réseau ({type(e).__name__}: {e}) — tentative {attempt}/{max_retries}, "
                  f"nouvelle tentative dans {wait}s...")
            time.sleep(wait)

    raise RuntimeError(f"Échec après {max_retries} tentatives sur l'API GraphQL GitHub.")


# ---------------------------------------------------------------------------
# 2. Stockage SQLite
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL")  # lecture/écriture concurrentes sans blocage mutuel
    conn.execute("""
        CREATE TABLE IF NOT EXISTS repos (
            id INTEGER,          -- repère numérique incrémental (ordre de collecte)
            name TEXT PRIMARY KEY,
            url TEXT,
            description TEXT,
            content_language TEXT,   -- code langue détecté (ex: 'en', 'fr')
            readme_compressed BLOB,
            readme_length INTEGER,
            stars INTEGER,
            forks INTEGER,
            primary_language TEXT,
            license TEXT,
            topics TEXT,        -- JSON-encodé, ex: '["cli", "python"]'
            created_at TEXT,
            pushed_at TEXT,
            archived INTEGER,
            last_fetched_at TEXT
        )
    """)
    # Migration : ajoute les colonnes aux bases créées avant leur introduction.
    existing_cols = [row[1] for row in conn.execute("PRAGMA table_info(repos)").fetchall()]
    if "content_language" not in existing_cols:
        conn.execute("ALTER TABLE repos ADD COLUMN content_language TEXT")
    if "id" not in existing_cols:
        conn.execute("ALTER TABLE repos ADD COLUMN id INTEGER")
    # Index pour filtrer rapidement le corpus avant de faire du RAG dessus
    # (ex: ne vectoriser que les repos avec beaucoup d'étoiles).
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stars ON repos(stars)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_content_language ON repos(content_language)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_id ON repos(id)")
    conn.commit()
    return conn


def repo_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute("SELECT 1 FROM repos WHERE name = ?", (name,))
    return cur.fetchone() is not None


def upsert_repo(conn: sqlite3.Connection, entry: dict):
    """
    Insère un nouveau repo ou met à jour un repo existant (par `name`).
    `id` est volontairement absent de la clause ON CONFLICT UPDATE : il n'est
    attribué qu'au tout premier INSERT et reste ensuite figé, même si ce
    repo est revisité (mis à jour) plus tard.
    """
    conn.execute("""
        INSERT INTO repos (
            id, name, url, description, content_language, readme_compressed, readme_length,
            stars, forks, primary_language, license, topics,
            created_at, pushed_at, archived, last_fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            url=excluded.url,
            description=excluded.description,
            content_language=excluded.content_language,
            readme_compressed=excluded.readme_compressed,
            readme_length=excluded.readme_length,
            stars=excluded.stars,
            forks=excluded.forks,
            primary_language=excluded.primary_language,
            license=excluded.license,
            topics=excluded.topics,
            created_at=excluded.created_at,
            pushed_at=excluded.pushed_at,
            archived=excluded.archived,
            last_fetched_at=excluded.last_fetched_at
    """, (
        entry["id"], entry["name"], entry["url"], entry["description"], entry["content_language"],
        entry["readme_compressed"], entry["readme_length"],
        entry["stars"], entry["forks"], entry["primary_language"],
        entry["license"], json.dumps(entry["topics"]),
        entry["created_at"], entry["pushed_at"], int(entry["archived"]),
        entry["last_fetched_at"],
    ))


class ThreadSafeCounter:
    """Compteur incrémental protégé par verrou, pour un usage sûr entre plusieurs threads."""
    def __init__(self, start: int):
        self._count = itertools.count(start)
        self._lock = threading.Lock()

    def __next__(self):
        with self._lock:
            return next(self._count)


def init_id_counter(conn: sqlite3.Connection):
    """
    Prépare un compteur d'id continu, qui repart après le plus grand id déjà
    attribué en base (fonctionne aussi bien sur une base neuve que sur une
    base existante, y compris si add_id_column.py a déjà tourné dessus).
    Thread-safe : peut être partagé entre plusieurs workers parallèles.
    """
    max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM repos").fetchone()[0]
    return ThreadSafeCounter(max_id + 1)


def compress_readme(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"))


def decompress_readme(blob: bytes) -> str:
    return gzip.decompress(blob).decode("utf-8")


def get_all_readmes(conn: sqlite3.Connection):
    """
    Générateur (name, readme_text) sur tout le corpus — c'est le point d'entrée
    à utiliser plus tard pour le chunking / l'indexation RAG, sans jamais
    charger tous les README en mémoire en même temps.
    """
    cur = conn.execute("SELECT name, readme_compressed FROM repos")
    for name, blob in cur:
        yield name, decompress_readme(blob)


# ---------------------------------------------------------------------------
# 3. État de pagination par segment (reprise entre les runs)
# ---------------------------------------------------------------------------

STAR_TIER_BOUNDS = [
    (50001, 100_000_000),  # équivalent à "stars:>50000"
    (10000, 49999),
    (5000, 9999),
    (1000, 4999),
    (500, 999),
    (100, 499),
    (0, 99),  # inclut désormais les repos avec peu ou pas d'étoiles
]

LICENSES = ["mit", "apache-2.0", "bsd-3-clause", "gpl-3.0", "unlicense"]

# Bornes de date par défaut pour le filtre "pushed:" (dernier commit).
# DATE_LOW_DEFAULT = date de création de GitHub -> équivaut à "aucune limite
# basse", tous les repos sont inclus peu importe la date de leur dernier commit.
# DATE_SENTINEL_HIGH est une date lointaine dans le futur, qui fait toujours
# office de "pas de limite haute" (les nouveaux commits futurs restent inclus
# sans avoir à recalculer "aujourd'hui" à chaque run).
DATE_LOW_DEFAULT = "2008-01-01"
DATE_SENTINEL_HIGH = "2030-01-01"
# - Le filtre "archived:false" est retiré : les repos archivés sont désormais
#   inclus aussi, pour maximiser le volume de contenu récupéré.
# - Pas de filtre "language:" : on inclut tous les langages (y compris rares)
#   et les repos sans langage de programmation dominant (documentation,
#   listes "awesome-*", données, configs...).

# GitHub plafonne CHAQUE recherche à 1000 résultats accessibles, même avec
# pagination complète. Si un segment dépasse ce seuil, une partie des repos
# correspondants resterait à jamais invisible. Pour éviter ça :
#   1. Un segment saturé est d'abord scindé en deux sous-tranches d'étoiles
#      plus fines (comme avant).
#   2. Si la tranche d'étoiles devient indivisible (une seule valeur, ex:
#      stars:0..0 — fréquent maintenant qu'on inclut les repos à 0 étoile,
#      où des dizaines de milliers de repos peuvent partager la même valeur)
#      mais reste saturée, on bascule sur une scission par PLAGE DE DATES de
#      dernier commit, divisible bien plus finement (jusqu'au jour près).
SEARCH_RESULT_CAP = 1000


def make_segment(low: int, high: int, license_,
                  date_low: str = DATE_LOW_DEFAULT, date_high: str = DATE_SENTINEL_HIGH) -> dict:
    license_label = license_ if license_ is not None else "any"
    label = f"stars_{low}_{high}__license_{license_label}"
    if date_low != DATE_LOW_DEFAULT or date_high != DATE_SENTINEL_HIGH:
        label += f"__pushed_{date_low}_{date_high}"
    return {
        "label": label,
        "low": low,
        "high": high,
        "license": license_,
        "date_low": date_low,
        "date_high": date_high,
        "cursor": None,
        "exhausted": False,
        "exhausted_at": None,
        "split": False,
        "repo_count": None,  # nombre réel de repos dans cet intervalle (connu dès la 1ère requête)
    }


def make_base_segments() -> dict:
    segments = {}
    for low, high in STAR_TIER_BOUNDS:
        for lic in LICENSES:
            seg = make_segment(low, high, lic)
            segments[seg["label"]] = seg
    return segments
# 7 tranches d'étoiles (dont 0-99) x 5 licences = 35 segments de départ,
# chacun scindable automatiquement (par étoiles puis par date si besoin) ->
# volume adressable non borné à l'avance, il s'adapte à la densité réelle
# des données GitHub, y compris sur la longue traîne des repos peu suivis.


def build_query(segment: dict) -> str:
    parts = [f"stars:{segment['low']}..{segment['high']}"]
    if segment["license"] is not None:
        parts.append(f"license:{segment['license']}")
    parts.append(f"pushed:{segment['date_low']}..{segment['date_high']}")
    parts.append("sort:stars-desc")
    return " ".join(parts)


# Un segment épuisé n'est utile à re-scanner qu'après un certain délai (le temps
# que de nouveaux repos passent le seuil d'étoiles de la tranche). Le rescanner
# à chaque run gaspille du budget pour très peu de nouveaux repos une fois le
# bassin initial couvert.
SEGMENT_RESCAN_COOLDOWN_HOURS = 24


def load_state(path: str = STATE_FILE) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict, path: str = STATE_FILE):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_segments(state: dict) -> dict:
    """Charge les segments depuis l'état persisté, ou les initialise au premier lancement."""
    if "segments" not in state:
        state["segments"] = make_base_segments()
    else:
        # Migration : complète les champs qui n'existaient pas encore quand
        # cet état a été sauvegardé (ex: date_low/date_high, ajoutés avec la
        # scission par date). Évite d'avoir à supprimer fetch_state.json à
        # chaque évolution du script.
        for seg in state["segments"].values():
            seg.setdefault("date_low", DATE_LOW_DEFAULT)
            seg.setdefault("date_high", DATE_SENTINEL_HIGH)
            seg.setdefault("repo_count", None)
            seg.setdefault("split", False)
            seg.setdefault("cursor", None)
            seg.setdefault("exhausted", False)
            seg.setdefault("exhausted_at", None)
    return state["segments"]


ACCEPTED_LANGUAGES = {"en", "fr"}

# langdetect n'est PAS thread-safe : sous accès concurrent, son détecteur
# partagé se corrompt et peut classifier un texte anglais comme polonais,
# finnois, etc. (vérifié empiriquement : ~64% de mauvaises détections avec
# 4 threads concurrents sur un même texte). Un verrou global sérialise les
# appels de détection -> coût négligeable (quelques ms), zéro risque de
# faux rejet dû à ce bug une fois la collecte parallélisée.
_LANGDETECT_LOCK = threading.Lock()


def detect_content_language(text: str) -> str | None:
    """
    Détecte la langue naturelle d'un texte (description ou extrait de README).
    Retourne le code langue détecté par langdetect (ex: 'en', 'fr', 'de'...),
    ou None si le texte est trop court ou indétectable.
    """
    if not text or len(text.strip()) < 20:
        return None
    try:
        with _LANGDETECT_LOCK:
            return detect(text)
    except LangDetectException:
        return None


# ---------------------------------------------------------------------------
# 4. Collecte d'un segment
# ---------------------------------------------------------------------------

def split_segment(segment: dict) -> list:
    """
    Scinde un segment saturé (>1000 résultats) pour réduire sa densité.
    Priorité 1 : scinder la tranche d'étoiles en deux, si elle est encore
    divisible (low < high).
    Priorité 2 (fallback) : si la tranche d'étoiles est déjà indivisible
    (low == high — fréquent sur les repos à 0 étoile), scinder par plage de
    dates de dernier commit à la place, en deux moitiés égales (en jours).
    Retourne une liste de 2 nouveaux segments, ou une liste vide si plus
    aucune scission n'est possible (cas extrême : une seule valeur d'étoile
    ET une seule journée, toujours saturé — le plafond de 1000 est alors
    accepté comme limite réelle et documentée).
    """
    low, high = segment["low"], segment["high"]

    if low < high:
        mid = low + (high - low) // 2
        child_low = make_segment(low, mid, segment["license"],
                                  segment["date_low"], segment["date_high"])
        child_high = make_segment(mid + 1, high, segment["license"],
                                   segment["date_low"], segment["date_high"])
        return [child_low, child_high]

    # Étoiles indivisibles : on tente de scinder par date à la place.
    date_low = date.fromisoformat(segment["date_low"])
    date_high = date.fromisoformat(segment["date_high"])
    span_days = (date_high - date_low).days

    if span_days < 1:
        return []  # indivisible sur les deux dimensions : limite réelle acceptée

    mid_offset = span_days // 2
    mid_date = date_low + timedelta(days=mid_offset)
    next_date = mid_date + timedelta(days=1)

    child_early = make_segment(low, high, segment["license"],
                                date_low.isoformat(), mid_date.isoformat())
    child_late = make_segment(low, high, segment["license"],
                               next_date.isoformat(), date_high.isoformat())
    return [child_early, child_late]


def fetch_segment(conn: sqlite3.Connection, segment: dict, safety_margin: int, id_counter):
    """
    Récupère des repos pour un segment donné jusqu'à ce que le rate limit
    restant descende sous `safety_margin`, que le segment soit épuisé, ou
    qu'une saturation (>1000 résultats réels) soit détectée.

    Retourne un dict avec les clés :
        added, updated, cursor, exhausted, rate_remaining, reset_at,
        stopped_on_rate_limit, split_children (liste de nouveaux segments,
        vide si pas de scission nécessaire).
    """
    query = build_query(segment)
    cursor = segment["cursor"]
    added = 0
    updated = 0
    exhausted = False
    stopped_on_rate_limit = False
    rate_remaining = None
    reset_at = None
    is_first_page = cursor is None
    nodes_seen = 0  # nombre de repos réellement parcourus (avant filtres langue/longueur)
    repo_count = None  # nombre total annoncé par GitHub pour cet intervalle

    while True:
        data = run_query(query, cursor)
        search_data = data["search"]
        rate = data["rateLimit"]
        rate_remaining = rate["remaining"]
        reset_at = rate["resetAt"]
        repo_count = search_data["repositoryCount"]

        # Affiché une seule fois par segment (constant sur toutes les pages
        # d'une même recherche) pour voir immédiatement combien de repos
        # existent réellement dans cet intervalle.
        if is_first_page:
            license_display = segment["license"] if segment["license"] is not None else "toutes/aucune"
            print(f"  [{segment['label']}] {repo_count} repos trouvés dans cet intervalle "
                  f"(stars:{segment['low']}..{segment['high']}, license:{license_display}, "
                  f"pushed:{segment['date_low']}..{segment['date_high']}).")

            if repo_count > SEARCH_RESULT_CAP:
                children = split_segment(segment)
                if children:
                    split_dim = "étoiles" if segment["low"] < segment["high"] else "dates"
                    print(f"  [{segment['label']}] saturé ({repo_count} > "
                          f"{SEARCH_RESULT_CAP}) — scission par {split_dim} en {len(children)} sous-segments.")
                    # On garde quand même les résultats de cette page (valides,
                    # juste redondants avec ce que les enfants retrouveront aussi).
                else:
                    print(f"  [{segment['label']}] saturé ({repo_count} résultats réels) mais "
                          f"totalement indivisible (1 seule valeur d'étoile ET 1 seule journée) — "
                          f"plafond de {SEARCH_RESULT_CAP} accepté comme limite réelle sur ce cas extrême.")
            is_first_page = False

        nodes_seen += len(search_data["nodes"])

        for repo in search_data["nodes"]:
            name = repo["nameWithOwner"]
            desc = repo.get("description") or ""

            readme = ""
            for key in ("readmeMd", "readmeLower", "readmeRst", "readmeNoExt"):
                blob = repo.get(key)
                if blob and blob.get("text"):
                    readme = blob["text"]
                    break

            if len(readme.strip()) < MIN_README_LENGTH:
                continue

            text_to_check = desc if desc else readme[:1000]
            content_lang = detect_content_language(text_to_check)
            if content_lang not in ACCEPTED_LANGUAGES:
                continue

            was_existing = repo_exists(conn, name)

            entry = {
                "id": next(id_counter),  # ignoré silencieusement par SQLite si `name` existe déjà
                "name": name,
                "url": repo["url"],
                "description": desc,
                "content_language": content_lang,
                "readme_compressed": compress_readme(readme),
                "readme_length": len(readme),
                "stars": repo["stargazerCount"],
                "forks": repo["forkCount"],
                "primary_language": (repo["primaryLanguage"] or {}).get("name"),
                "license": (repo["licenseInfo"] or {}).get("spdxId"),
                "topics": [t["topic"]["name"] for t in repo["repositoryTopics"]["nodes"]],
                "created_at": repo["createdAt"],
                "pushed_at": repo["pushedAt"],
                "archived": repo["isArchived"],
                "last_fetched_at": datetime.now(timezone.utc).isoformat(),
            }

            upsert_repo(conn, entry)

            if was_existing:
                updated += 1
            else:
                added += 1

        conn.commit()

        print(f"  [{segment['label']}] +{added} nouveaux, {updated} mis à jour "
              f"/ {nodes_seen}/{repo_count} repos parcourus "
              f"/ rate limit restant : {rate_remaining}")

        if repo_count > SEARCH_RESULT_CAP:
            # Segment saturé détecté ci-dessus : on arrête de le paginer lui-même
            # (les enfants scindés couvriront le terrain plus finement).
            children = split_segment(segment)
            return {
                "added": added, "updated": updated, "cursor": None, "exhausted": True,
                "rate_remaining": rate_remaining, "reset_at": reset_at,
                "stopped_on_rate_limit": False, "split_children": children,
                "repo_count": repo_count,
            }

        if not search_data["pageInfo"]["hasNextPage"]:
            exhausted = True
            cursor = None
            coverage = f"{nodes_seen}/{repo_count}" if repo_count else f"{nodes_seen}/?"
            complete = "COMPLET" if nodes_seen >= repo_count else "INCOMPLET (?)"
            print(f"  [{segment['label']}] segment épuisé — couverture {coverage} "
                  f"({complete}), sera repris depuis le début plus tard.")
            break

        cursor = search_data["pageInfo"]["endCursor"]

        if rate_remaining < safety_margin:
            stopped_on_rate_limit = True
            print(f"  [{segment['label']}] rate limit sous le seuil de sécurité "
                  f"({rate_remaining} < {safety_margin}), pause de ce segment.")
            break

    return {
        "added": added, "updated": updated, "cursor": cursor, "exhausted": exhausted,
        "rate_remaining": rate_remaining, "reset_at": reset_at,
        "stopped_on_rate_limit": stopped_on_rate_limit, "split_children": [],
        "repo_count": repo_count,
    }


def parse_reset_at(reset_at_str: str) -> datetime:
    return datetime.fromisoformat(reset_at_str.replace("Z", "+00:00"))


def run_one_cycle(conn: sqlite3.Connection, state: dict, safety_margin: int, id_counter,
                   max_workers: int = 1):
    """
    Parcourt les segments (hors cooldown) jusqu'à ce que le rate limit
    descende sous `safety_margin`. Les segments saturés sont scindés à la
    volée et leurs enfants rejoignent la file de traitement du même cycle.

    `max_workers` > 1 distribue le traitement sur plusieurs threads, chacun
    avec sa propre connexion SQLite (SQLite sérialise lui-même les écritures
    concurrentes, pas besoin de verrou manuel dessus). Le rate limit reste
    partagé entre tous les threads (même budget total, juste consommé plus
    vite en temps d'horloge grâce au recouvrement des latences réseau).

    `conn` (la connexion passée en argument) n'est utilisée que si
    max_workers == 1 ; au-delà, chaque worker ouvre la sienne.

    Retourne (total_added, total_updated, reset_at).
    """
    segments = get_segments(state)

    queue = collections.deque()
    for label, seg in segments.items():
        if seg.get("split"):
            continue
        if seg.get("exhausted") and seg.get("exhausted_at"):
            exhausted_since = datetime.fromisoformat(seg["exhausted_at"])
            hours_since = (datetime.now(timezone.utc) - exhausted_since).total_seconds() / 3600
            if hours_since < SEGMENT_RESCAN_COOLDOWN_HOURS:
                continue
        queue.append(label)

    queue_lock = threading.Lock()
    state_lock = threading.Lock()
    stop_event = threading.Event()

    totals = {"added": 0, "updated": 0}
    last_reset_at = [None]
    any_segment_processed = [False]

    def worker():
        worker_conn = get_db()  # créée DANS ce thread — SQLite l'exige
        try:
            while not stop_event.is_set():
                with queue_lock:
                    if not queue:
                        return
                    label = queue.popleft()
                    segment = segments[label]

                print(f"Segment '{label}' — reprise au curseur : {segment['cursor'] or 'début'}")

                try:
                    result = fetch_segment(worker_conn, segment, safety_margin, id_counter)
                except Exception as e:
                    print(f"Segment '{label}' — échec : {e}. Passage au segment suivant.")
                    with state_lock:
                        save_state(state)
                    continue

                with state_lock:
                    any_segment_processed[0] = True
                    last_reset_at[0] = result["reset_at"]
                    segment["repo_count"] = result.get("repo_count")

                    if result["split_children"]:
                        segment["split"] = True
                        segment["exhausted"] = True
                        segment["exhausted_at"] = datetime.now(timezone.utc).isoformat()
                        with queue_lock:
                            for child in result["split_children"]:
                                segments[child["label"]] = child
                                queue.append(child["label"])
                    else:
                        segment["cursor"] = result["cursor"]
                        segment["exhausted"] = result["exhausted"]
                        segment["exhausted_at"] = (
                            datetime.now(timezone.utc).isoformat() if result["exhausted"] else None
                        )

                    totals["added"] += result["added"]
                    totals["updated"] += result["updated"]
                    save_state(state)

                if result["stopped_on_rate_limit"]:
                    print("Rate limit épuisé pour ce cycle (seuil de sécurité atteint).")
                    stop_event.set()
                    return
        finally:
            worker_conn.close()

    if max_workers <= 1:
        # Cas séquentiel : réutilise directement la connexion fournie, pas besoin
        # d'en ouvrir une nouvelle dans un thread séparé.
        while not stop_event.is_set():
            with queue_lock:
                if not queue:
                    break
                label = queue.popleft()
                segment = segments[label]

            print(f"Segment '{label}' — reprise au curseur : {segment['cursor'] or 'début'}")

            try:
                result = fetch_segment(conn, segment, safety_margin, id_counter)
            except Exception as e:
                print(f"Segment '{label}' — échec : {e}. Passage au segment suivant.")
                save_state(state)
                continue

            any_segment_processed[0] = True
            last_reset_at[0] = result["reset_at"]
            segment["repo_count"] = result.get("repo_count")

            if result["split_children"]:
                segment["split"] = True
                segment["exhausted"] = True
                segment["exhausted_at"] = datetime.now(timezone.utc).isoformat()
                for child in result["split_children"]:
                    segments[child["label"]] = child
                    queue.append(child["label"])
            else:
                segment["cursor"] = result["cursor"]
                segment["exhausted"] = result["exhausted"]
                segment["exhausted_at"] = (
                    datetime.now(timezone.utc).isoformat() if result["exhausted"] else None
                )

            totals["added"] += result["added"]
            totals["updated"] += result["updated"]
            save_state(state)

            if result["stopped_on_rate_limit"]:
                print("Rate limit épuisé pour ce cycle (seuil de sécurité atteint).")
                break
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(worker) for _ in range(max_workers)]
            for f in futures:
                f.result()  # remonte toute exception inattendue d'un worker

    if not any_segment_processed[0]:
        print("Aucun segment disponible ce cycle (tous en cooldown).")

    return totals["added"], totals["updated"], last_reset_at[0]


if __name__ == "__main__":
    # Nombre de threads travaillant en parallèle sur des segments différents.
    # Le rate limit (5000 points/heure) reste partagé entre tous les workers
    # -> ça ne donne pas plus de budget, ça consomme juste le même budget
    # plus vite en temps d'horloge (moins de temps passé à attendre les
    # réponses réseau les unes après les autres). Une valeur entre 5 et 10
    # est un bon compromis ; au-delà, GitHub peut renvoyer plus de 502/504
    # sous forte charge concurrente.
    # Le vrai garde-fou contre le rate limit secondaire est désormais
    # GRAPHQL_RATE_LIMITER (plafond explicite de requêtes/minute, partagé
    # entre tous les threads) — plus besoin de deviner un MAX_WORKERS "sûr"
    # au hasard. On peut donc remonter ce nombre pour mieux recouvrir la
    # latence réseau ; la limite de 100 requêtes simultanées documentée par
    # GitHub laisse largement la place.
    MAX_WORKERS = 12

    # Marge de sécurité : on arrête de consommer avant d'atteindre 0, pour
    # laisser de la marge à d'autres usages ponctuels du même token
    # (ex: check_rate_limit.py, une requête manuelle...). Avec plusieurs
    # workers, chacun peut consommer un peu avant de voir que le seuil est
    # atteint -> la marge est élargie proportionnellement pour éviter de
    # dépasser 0 avant que tous les threads ne s'arrêtent.
    RATE_LIMIT_SAFETY_MARGIN = 200 * max(MAX_WORKERS, 1)

    # Si tous les segments sont en cooldown (rien à faire) alors que le rate
    # limit est disponible, on ne reste pas bloqué : on retente après ce délai.
    IDLE_RETRY_MINUTES = 15

    # Marge ajoutée après resetAt, au cas où l'horloge du serveur et celle de
    # GitHub ne seraient pas parfaitement synchronisées.
    RESET_BUFFER_SECONDS = 30

    conn = get_db()
    id_counter = init_id_counter(conn)

    print(f"Démarrage en boucle continue ({MAX_WORKERS} worker(s) en parallèle, "
          f"marge de sécurité : {RATE_LIMIT_SAFETY_MARGIN} points).")
    print("Ctrl+C pour arrêter proprement, ou lance ce script via systemd/tmux/screen "
          "pour qu'il tourne en tâche de fond.\n")

    while True:
        state = load_state()

        total_count = conn.execute("SELECT COUNT(*) FROM repos").fetchone()[0]
        print(f"{total_count} repos déjà présents dans {DB_FILE}")

        added, updated, reset_at = run_one_cycle(
            conn, state, RATE_LIMIT_SAFETY_MARGIN, id_counter, max_workers=MAX_WORKERS
        )

        final_count = conn.execute("SELECT COUNT(*) FROM repos").fetchone()[0]
        print(f"\nCycle terminé : {added} nouveaux repos ajoutés, {updated} mis à jour.")
        print(f"Total dans {DB_FILE} : {final_count} repos")
        # Ligne au format fixe, facile à parser plus tard pour mesurer la
        # vitesse réelle d'accumulation (voir estimate_time_to_target.py).
        print(f"PROGRESS {datetime.now(timezone.utc).isoformat()} total={final_count}\n")

        if reset_at:
            reset_dt = parse_reset_at(reset_at)
            sleep_seconds = (reset_dt - datetime.now(timezone.utc)).total_seconds() + RESET_BUFFER_SECONDS
            sleep_seconds = max(sleep_seconds, 10)  # jamais une pause négative ou nulle
            print(f"Prochaine reprise à {reset_dt.isoformat()} "
                  f"(pause de {sleep_seconds / 60:.1f} min)...\n")
        else:
            # Aucun segment traité ce cycle (tous en cooldown) : on retente
            # simplement après un court délai plutôt que d'attendre une heure.
            sleep_seconds = IDLE_RETRY_MINUTES * 60
            print(f"Rien à faire ce cycle, nouvelle tentative dans {IDLE_RETRY_MINUTES} min...\n")

        time.sleep(sleep_seconds)