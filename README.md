# GitHub GraphQL Collector

Service de collecte GitHub fonctionnant en continu dans Docker. Récupère les repositories publics ayant au moins 1000 stars, collecte leur historique de commits, puis surveille en continu leurs évolutions. Toutes les données sont stockées dans PostgreSQL.

---

## Architecture

```
GitHub GraphQL API
        ↓
Docker container
        ↓
  Python Collector
  ├── main.py              — orchestrateur
  ├── config.py            — variables d'environnement
  ├── database/
  │   ├── connection.py    — connexion PostgreSQL
  │   └── schema.py        — création des schemas/tables
  ├── github/
  │   ├── client.py        — client GraphQL (retry, rate limit)
  │   ├── queries.py       — requêtes GraphQL centralisées
  ├── monitoring/
  │   └── rate_limit.py    — monitoring consommation API
  └── services/
      ├── historical.py    — collecte historique initiale
      └── watcher.py       — surveillance continue
        ↓
    PostgreSQL
    ├── schema github
    │   ├── repositories
    │   ├── commits
    │   └── repository_snapshots
    └── schema monitoring
        └── rate_limit_usage
```

---

## Prérequis

- Docker Desktop
- Token GitHub avec scope `public_repo` (lecture seule)

### Créer un token GitHub

1. Aller sur [github.com/settings/tokens](https://github.com/settings/tokens)
2. Cliquer sur **Generate new token (classic)**
3. Cocher uniquement `public_repo`
4. Copier le token généré

---

## Configuration

```bash
cp .env.example .env
```

Renseigner le token dans `.env` :

```
GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx

POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=github_data
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

PAGE_SIZE=100
MAX_REPOSITORIES=0        # 0 = pas de limite
POLL_INTERVAL_SECONDS=300
LOG_LEVEL=INFO
```

---

## Lancement Docker

```bash
docker compose up --build
```

Le système va alors :
1. Démarrer PostgreSQL
2. Créer les schemas et tables
3. Démarrer la collecte historique
4. Passer automatiquement en mode surveillance

Pour lancer en arrière-plan :

```bash
docker compose up --build -d
docker compose logs -f collector
```

Pour arrêter proprement :

```bash
docker compose stop
```

---

## Collecte historique

Au premier démarrage, le collector parcourt des intervalles de stars pour contourner la limite GitHub de 1000 résultats par requête :

```
stars:1000..1999  →  paginer jusqu'à 1000 repos
stars:2000..2999  →  paginer jusqu'à 1000 repos
...
stars:50000..99999
stars:100000..499999
stars:>=500000
```

Pour chaque repository, l'historique complet de commits est collecté du plus ancien au plus récent.

**Idempotence** : si le container redémarre, les repositories et commits déjà collectés sont ignorés (`ON CONFLICT DO NOTHING` sur la clé `(repository_id, sha)`).

---

## Mode surveillance (WATCH)

Après la collecte historique, le collector entre dans une boucle de surveillance :

```
attendre POLL_INTERVAL_SECONDS
        ↓
pour chaque repository connu
        ↓
dernier commit GitHub == last_seen_commit_sha ?
   non → récupérer commits manquants → insérer → snapshot
   oui → pas d'action
```

Seuls les commits **nouveaux** sont récupérés (collection incrémentale).

---

## Structure PostgreSQL

### `github.repositories`

État courant connu de chaque repository.

| Colonne | Description |
|---|---|
| `id` | ID GitHub (stable) |
| `full_name` | owner/name |
| `stars`, `forks` | métriques courantes |
| `last_seen_commit_sha` | dernier commit collecté |
| `last_checked_at` | dernière vérification |

### `github.repository_snapshots`

Évolution des métriques dans le temps. Un snapshot est créé uniquement lorsque stars, forks, description ou language ont changé.

### `monitoring.rate_limit_usage`

Consommation de l'API GitHub GraphQL, enregistrée à chaque requête.

---

## Monitoring du rate limit

Le collector analyse chaque réponse GraphQL :
- enregistre la consommation dans `monitoring.rate_limit_usage`
- si `remaining < 100`, attend automatiquement jusqu'au reset

---

## Commandes utiles

```sql
-- Nombre de repositories collectés
SELECT COUNT(*) FROM github.repositories;

-- Top 10 repositories par stars
SELECT full_name, stars, forks, language
FROM github.repositories
ORDER BY stars DESC
LIMIT 10;

-- Les 20 dépôts avec les mises à jour les plus récentes
SELECT full_name, updated_at
FROM  github.repositories 
ORDER BY updated_at DESC
LIMIT 20;

-- Évolution d'un repository
SELECT snapshot_at, stars, forks
FROM github.repository_snapshots
WHERE repository_id = 'R_...'
ORDER BY snapshot_at;

-- Consommation API récente
SELECT * FROM monitoring.rate_limit_usage
ORDER BY timestamp DESC
LIMIT 20;

-- Consommation par heure
SELECT date_trunc('hour', timestamp) AS heure,
       COUNT(*) AS requetes,
       SUM(cost) AS cout_total,
       MIN(remaining) AS remaining_min
FROM monitoring.rate_limit_usage
GROUP BY 1
ORDER BY 1 DESC;

-- Opérations les plus coûteuses
SELECT operation_name,
       COUNT(*) AS appels,
       AVG(cost)::numeric(5,2) AS cout_moyen,
       SUM(cost) AS cout_total
FROM monitoring.rate_limit_usage
GROUP BY operation_name
ORDER BY cout_total DESC;
```

Se connecter à PostgreSQL :

```bash
docker exec -it github_postgres psql -U postgres -d github_data
```

---

## Tests

```bash
pip install pytest pytest-mock
cd tests
pytest -v
```

---

## Redémarrage après arrêt brutal

Le collector reprend automatiquement là où il s'était arrêté :
- les repositories déjà collectés sont mis à jour (UPSERT)
- les commits déjà insérés sont ignorés (`ON CONFLICT DO NOTHING`)
- la surveillance repart depuis le dernier `last_seen_commit_sha` stocké en base
