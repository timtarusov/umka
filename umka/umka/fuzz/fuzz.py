"""
Module for extendtins polars dataframe with fuzzy matching
"""
import polars as pl

from .umka_rs import (fuzz_partial_ratio, fuzz_ratio, fuzz_token_set_ratio,
                      fuzz_token_sort_ratio)


@pl.api.register_dataframe_namespace("fuzzy")
class FuzzyNamespace:
    """
    Fuzzy matching namespace
    """

    def __init__(self, dataframe: pl.DataFrame) -> None:
        self._df = dataframe

    def ratio(self, col1: str, col2: str) -> pl.DataFrame:
        """
        Fuzzy ratio
        """
        return fuzz_ratio(self._df, col1, col2)

    def partial_ratio(self, col1: str, col2: str) -> pl.DataFrame:
        """
        Fuzzy partial ratio
        """
        return fuzz_partial_ratio(self._df, col1, col2)

    def token_sort_ratio(self, col1: str, col2: str) -> pl.DataFrame:
        """
        Fuzzy token sort ratio
        """
        return fuzz_token_sort_ratio(self._df, col1, col2)

    def token_set_ratio(self, col1: str, col2: str) -> pl.DataFrame:
        """
        Fuzzy token set ratio
        """
        return fuzz_token_set_ratio(self._df, col1, col2)
