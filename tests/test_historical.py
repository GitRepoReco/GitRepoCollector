"""Tests de la génération des intervalles de stars par tiers."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

os.environ.setdefault("GITHUB_TOKEN", "fake_token_for_tests")

from services.historical import _build_tier_ranges, _search_query_for_tier


class TestStarRanges:

    def test_premier_tier_sans_borne_superieure(self):
        ranges = _build_tier_ranges([100000, 50000, 1000])
        assert ranges[0] == (100000, None)

    def test_deuxieme_tier_borne_haute_est_tier_precedent_moins_1(self):
        ranges = _build_tier_ranges([100000, 50000, 1000])
        assert ranges[1] == (50000, 99999)

    def test_dernier_tier_borne_haute_correcte(self):
        ranges = _build_tier_ranges([100000, 50000, 1000])
        assert ranges[2] == (1000, 49999)

    def test_pas_de_chevauchement(self):
        ranges = _build_tier_ranges([100000, 75000, 50000, 1000])
        for i in range(len(ranges) - 1):
            current_low = ranges[i][0]
            next_high   = ranges[i + 1][1]
            # Le tier suivant doit se terminer strictement en dessous du tier courant
            if next_high is not None:
                assert next_high < current_low, (
                    f"Chevauchement entre {ranges[i]} et {ranges[i+1]}"
                )

    def test_tous_les_tiers_couverts(self):
        tiers = [100000, 75000, 50000, 25000, 1000]
        ranges = _build_tier_ranges(tiers)
        assert len(ranges) == len(tiers)

    def test_query_avec_borne_haute(self):
        assert _search_query_for_tier(1000, 1999) == "stars:1000..1999 sort:created-asc"

    def test_query_sans_borne_haute(self):
        assert _search_query_for_tier(100000, None) == "stars:>=100000 sort:created-asc"

    def test_tiers_default_commencent_au_plus_eleve(self):
        from config import STAR_TIERS
        assert STAR_TIERS[0] == max(STAR_TIERS)

