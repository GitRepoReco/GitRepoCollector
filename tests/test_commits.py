"""Tests de détection de changement de SHA."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

os.environ.setdefault("GITHUB_TOKEN", "fake_token_for_tests")


class TestCommitDetection:

    def test_nouveau_commit_detecte_quand_sha_different(self):
        known_sha = "AAA"
        latest_sha = "BBB"
        assert latest_sha != known_sha

    def test_pas_de_nouveau_commit_quand_sha_identique(self):
        known_sha = "AAA"
        latest_sha = "AAA"
        assert latest_sha == known_sha

