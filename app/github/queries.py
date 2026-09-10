# Toutes les requêtes GraphQL GitHub centralisées ici.

# Coût : 1 point par page (scalaires + objets imbriqués uniquement, pas de connexions imbriquées)
QUERY_SEARCH_REPOSITORIES = """
query SearchRepositories(
    $query: String!
    $first: Int!
    $after: String
) {
    search(query: $query, type: REPOSITORY, first: $first, after: $after) {
        repositoryCount
        pageInfo {
            hasNextPage
            endCursor
        }
        nodes {
            ... on Repository {
                id
                name
                nameWithOwner
                description
                url
                homepageUrl
                openGraphImageUrl
                stargazerCount
                forkCount
                diskUsage
                visibility
                isArchived
                isFork
                isTemplate
                isDisabled
                isMirror
                mirrorUrl
                hasIssuesEnabled
                hasWikiEnabled
                hasDiscussionsEnabled
                mergeCommitAllowed
                squashMergeAllowed
                rebaseMergeAllowed
                deleteBranchOnMerge
                createdAt
                updatedAt
                pushedAt
                sshUrl
                primaryLanguage { name color }
                defaultBranchRef { name }
                owner { login avatarUrl }
                licenseInfo { name spdxId }
                parent { nameWithOwner url }
                codeOfConduct { name url }
            }
        }
    }
    rateLimit { limit cost remaining resetAt }
}
"""

# Coût : 1 point par repo (connexions à first réduit, reste dans le minimum)
QUERY_REPOSITORY_README = """
query RepositoryReadme($owner: String!, $name: String!) {
    repository(owner: $owner, name: $name) {
        defaultBranchRef {
            target {
                ... on Commit { oid }
            }
        }
        object(expression: "HEAD:README.md") {
            ... on Blob { text }
        }
        releases { totalCount }
        openIssues:   issues(states: OPEN)   { totalCount }
        closedIssues: issues(states: CLOSED) { totalCount }
        openPRs:   pullRequests(states: OPEN)   { totalCount }
        mergedPRs: pullRequests(states: MERGED) { totalCount }
        watchers { totalCount }
        repositoryTopics(first: 30) { nodes { topic { name } } }
        languages(first: 10) { totalSize nodes { name color } }
    }
    rateLimit { limit cost remaining resetAt }
}
"""

# Coût : 1 point par repo (identique à QUERY_REPOSITORY_README mais sans README)
# Utilisé pour paralléliser : REST GET pour README + GraphQL pour stats
QUERY_REPOSITORY_STATS = """
query RepositoryStats($owner: String!, $name: String!) {
    repository(owner: $owner, name: $name) {
        defaultBranchRef {
            target {
                ... on Commit { oid }
            }
        }
        releases { totalCount }
        openIssues:   issues(states: OPEN)   { totalCount }
        closedIssues: issues(states: CLOSED) { totalCount }
        openPRs:   pullRequests(states: OPEN)   { totalCount }
        mergedPRs: pullRequests(states: MERGED) { totalCount }
        watchers { totalCount }
        repositoryTopics(first: 30) { nodes { topic { name } } }
        languages(first: 10) { totalSize nodes { name color } }
    }
    rateLimit { limit cost remaining resetAt }
}
"""

# Coût : 1 point par page (léger, aucun nœud retourné)
# Utilisé pour vérifier si le nombre total de résultats dépasse 1000 (limite GitHub)
QUERY_SEARCH_COUNT = """
query SearchCount($query: String!) {
    search(query: $query, type: REPOSITORY, first: 1) {
        repositoryCount
    }
    rateLimit { limit cost remaining resetAt }
}
"""

