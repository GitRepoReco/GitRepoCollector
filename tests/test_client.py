"""Tests du GitHubGraphQLClient — parsing et retry."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import pytest
from unittest.mock import MagicMock, patch

os.environ.setdefault("GITHUB_TOKEN", "fake_token_for_tests")


from github.client import GitHubGraphQLClient


def _mock_response(status_code: int = 200, json_data: dict = None, text: str = "", headers: dict = None):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data or {}
    mock.text = text
    mock.headers = headers or {}
    return mock


class TestExecuteGraphQL:

    def setup_method(self):
        self.client = GitHubGraphQLClient()

    def test_retourne_data_et_rate_limit(self):
        payload = {
            "data": {
                "search": {"nodes": [{"id": "R_1"}]},
                "rateLimit": {"limit": 5000, "remaining": 4980, "cost": 1, "resetAt": "2026-01-01T00:00:00Z"},
            }
        }
        headers = {"x-ratelimit-limit": "5000", "x-ratelimit-remaining": "4980",
                   "x-ratelimit-used": "20", "x-ratelimit-reset": "9999999999"}
        with patch("requests.post", return_value=_mock_response(200, payload, headers=headers)):
            data, rate_limit = self.client.execute("query {}", {})

        assert data["search"]["nodes"][0]["id"] == "R_1"
        # Les headers ont la priorité sur le champ GraphQL
        assert rate_limit["remaining"] == 4980
        assert rate_limit["used"] == 20
        assert rate_limit["cost"] == 1

    def test_erreur_graphql_leve_exception(self):
        payload = {"errors": [{"message": "Unauthorized"}]}
        with patch("requests.post", return_value=_mock_response(200, payload)):
            with pytest.raises(RuntimeError, match="Erreur GraphQL"):
                self.client.execute("query {}", {})

    def test_erreur_graphql_transitoire_retry_puis_succes(self):
        transient_payload = {
            "errors": [
                {
                    "message": (
                        "Something went wrong while executing your query on GitHub"
                    )
                }
            ]
        }
        success_payload = {"data": {"rateLimit": {"remaining": 100}}}
        headers_ok = {"x-ratelimit-remaining": "100", "x-ratelimit-reset": "9999999999"}
        responses = [
            _mock_response(200, transient_payload),
            _mock_response(200, success_payload, headers=headers_ok),
        ]
        with patch("requests.post", side_effect=responses):
            with patch("time.sleep") as mock_sleep:
                data, _ = self.client.execute("query {}", {})

        assert data is not None
        mock_sleep.assert_called_once()

    def test_http_502_retry_puis_succes(self):
        success_payload = {"data": {"rateLimit": {"remaining": 100}}}
        headers_ok = {"x-ratelimit-remaining": "100", "x-ratelimit-reset": "9999999999"}
        responses = [
            _mock_response(502, text="Bad Gateway"),
            _mock_response(200, success_payload, headers=headers_ok),
        ]
        with patch("requests.post", side_effect=responses):
            with patch("time.sleep"):
                data, _ = self.client.execute("query {}", {})
        assert data is not None

    def test_http_502_echec_apres_max_retries(self):
        with patch("requests.post", return_value=_mock_response(502, text="Bad Gateway")):
            with patch("time.sleep"):
                with pytest.raises(RuntimeError, match="tentatives"):
                    self.client.execute("query {}", {})

    def test_http_401_echec_immediat_sans_retry(self):
        call_count = 0

        def counting_post(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return _mock_response(401, text="Unauthorized")

        with patch("requests.post", side_effect=counting_post):
            with pytest.raises(RuntimeError, match="HTTP 401"):
                self.client.execute("query {}", {})

        assert call_count == 1

    def test_http_403_secondary_rate_limit_retry_after(self):
        success_payload = {"data": {"rateLimit": {}}}
        headers_ok = {"x-ratelimit-remaining": "500", "x-ratelimit-reset": "9999999999"}
        responses = [
            _mock_response(403, headers={"retry-after": "30"}),
            _mock_response(200, success_payload, headers=headers_ok),
        ]
        with patch("requests.post", side_effect=responses):
            with patch("time.sleep") as mock_sleep:
                self.client.execute("query {}", {})
        mock_sleep.assert_called_once_with(30)

    def test_http_403_primary_rate_limit_attend_reset(self):
        success_payload = {"data": {"rateLimit": {}}}
        headers_ok = {"x-ratelimit-remaining": "500", "x-ratelimit-reset": "9999999999"}
        responses = [
            _mock_response(403, headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "9999999999"}),
            _mock_response(200, success_payload, headers=headers_ok),
        ]
        with patch("requests.post", side_effect=responses):
            with patch("time.sleep") as mock_sleep:
                self.client.execute("query {}", {})
        # Doit avoir attendu (valeur >= 5 : margin + temps jusqu'au reset)
        assert mock_sleep.called
