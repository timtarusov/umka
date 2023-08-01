import polars as pl
import pytest

import umka


@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame({"a": ["foo", "bar", "baz"], "b": ["foo", "bar", "baz"]})


class TestFuzz:
    def test_ratio(self, df):
        ratio = df.fuzzy.ratio("a", "b")
        assert (
            ratio.select("ratio")
            .to_series()
            .series_equal(pl.Series("ratio", [100.0, 100.0, 100.0]))
        )

    def test_partial_ratio(self, df):
        ratio = df.fuzzy.partial_ratio("a", "b")
        assert (
            ratio.select("partial_ratio")
            .to_series()
            .series_equal(pl.Series("partial_ratio", [100.0, 100.0, 100.0]))
        )

    def test_token_sort_ratio(self, df):
        ratio = df.fuzzy.token_sort_ratio("a", "b")
        assert (
            ratio.select("token_sort_ratio")
            .to_series()
            .series_equal(pl.Series("token_sort_ratio", [100.0, 100.0, 100.0]))
        )

    def test_token_set_ratio(self, df):
        ratio = df.fuzzy.token_set_ratio("a", "b")
        assert (
            ratio.select("token_set_ratio")
            .to_series()
            .series_equal(pl.Series("token_set_ratio", [100.0, 100.0, 100.0]))
        )
