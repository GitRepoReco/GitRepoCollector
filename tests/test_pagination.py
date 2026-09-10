"""Tests de la pagination générique du client."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

os.environ.setdefault("GITHUB_TOKEN", "fake_token_for_tests")

from unittest.mock import patch, MagicMock
from github.client import GitHubGraphQLClient


def _page(nodes, has_next, cursor):
    return {
        "data": {
            "search": {
                "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                "nodes": nodes,
            },
            "rateLimit": {"remaining": 5000, "cost": 1},
        }
    }


def _mock_response(json_data):
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = json_data
    return mock


class TestPaginate:

    def setup_method(self):
        self.client = GitHubGraphQLClient()

    def test_two_pages_yields_all_nodes(self):
        page1 = _page([{"id": "R_1"}, {"id": "R_2"}], has_next=True, cursor="cursor_1")
        page2 = _page([{"id": "R_3"}], has_next=False, cursor=None)

        with patch("requests.post", side_effect=[_mock_response(page1), _mock_response(page2)]):
            results = []
            for nodes, _ in self.client.paginate(
                "query {}",
                {"query": "stars:>=1000"},
                page_info_path=["search", "pageInfo"],
                nodes_path=["search", "nodes"],
            ):
                results.extend(nodes)

        assert len(results) == 3
        assert results[0]["id"] == "R_1"
        assert results[2]["id"] == "R_3"

    def test_single_page_stops_correctly(self):
        page1 = _page([{"id": "R_1"}], has_next=False, cursor=None)

        with patch("requests.post", return_value=_mock_response(page1)):
            pages = list(self.client.paginate(
                "query {}",
                {"query": "stars:>=1000"},
                page_info_path=["search", "pageInfo"],
                nodes_path=["search", "nodes"],
            ))

        assert len(pages) == 1
        assert pages[0][0][0]["id"] == "R_1"

    def test_cursor_is_passed_on_second_call(self):
        page1 = _page([{"id": "R_1"}], has_next=True, cursor="abc123")
        page2 = _page([{"id": "R_2"}], has_next=False, cursor=None)

        calls = []

        def capture_call(*args, **kwargs):
            calls.append(kwargs.get("json", {}).get("variables", {}))
            if len(calls) == 1:
                return _mock_response(page1)
            return _mock_response(page2)

        with patch("requests.post", side_effect=capture_call):
            list(self.client.paginate(
                "query {}",
                {"query": "stars:>=1000"},
                page_info_path=["search", "pageInfo"],
                nodes_path=["search", "nodes"],
            ))

        assert calls[0]["after"] is None
        assert calls[1]["after"] == "abc123"
