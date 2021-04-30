import pytest

from src.calculators import TermFrequencyCalculator


class TestTermFrequencyCalculator:
    @staticmethod
    @pytest.mark.parametrize("bad_term_count", [0, -1, -0.2, 3.2, 1.0])
    def test_bad_term_count(bad_term_count) -> None:
        """Raise value error when term counts is not a positive integer."""
        # === Arrange, Act, & Assert
        with pytest.raises(ValueError, match=".*must be a positive integer.*"):
            TermFrequencyCalculator(
                term_count=bad_term_count, terms_in_document=1, max_term_count=1
            )

    @staticmethod
    @pytest.mark.parametrize("bad_terms_in_document", [0, -1, -0.2, 3.2, 1.0])
    def test_bad_terms_in_document(bad_terms_in_document) -> None:
        """Raise value error when terms in document is not a positive integer."""
        # === Arrange, Act, & Assert
        with pytest.raises(ValueError, match=".*must be a positive integer.*"):
            TermFrequencyCalculator(
                term_count=1, terms_in_document=bad_terms_in_document, max_term_count=1
            )

    @staticmethod
    @pytest.mark.parametrize("bad_max_term_count", [0, -1, -0.2, 3.2, 1.0])
    def test_bad_max_term_count(bad_max_term_count) -> None:
        """Raise value error when max term count is not a positive integer."""
        # === Arrange, Act, & Assert
        with pytest.raises(ValueError, match=".*must be a positive integer.*"):
            TermFrequencyCalculator(
                term_count=1, terms_in_document=1, max_term_count=bad_max_term_count
            )

    @staticmethod
    @pytest.mark.parametrize("bad_k", [0, 1, -0.2, 3.2, 1.0, 100])
    def test_bad_k(bad_k) -> None:
        """Raise value error when max term count is not a positive integer."""
        # === Arrange
        object_under_test = TermFrequencyCalculator(
            term_count=1, terms_in_document=1, max_term_count=1
        )
        # === Act, & Assert
        with pytest.raises(ValueError, match=".*must be positive but less than 1.*"):
            object_under_test.get_double_k_normalization(k=bad_k)
