import pytest

from src.calculators import *


class TestTermFrequencyCalculator:
    @staticmethod
    def test_integration() -> None:
        """Test integration."""
        # === Arrange, Act, & Assert
        TermFrequencyCalculator(term_count=1, terms_in_document=1, max_term_count=1)
        assert True

    @staticmethod
    def test_integration_with_k_norm() -> None:
        """Test integration with k normalization."""
        # === Arrange, Act, & Assert
        object_under_test = TermFrequencyCalculator(
            term_count=1, terms_in_document=1, max_term_count=1
        )
        object_under_test.get_double_k_normalization(k=0.5)
        assert True

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


class TestInverseDocumentFrequencyCalculator:
    @staticmethod
    def test_integration() -> None:
        """Test integration."""
        # === Arrange, Act, & Assert
        InverseDocumentFrequencyCalculator(
            documents_with_term=1, documents_in_corpus=1, max_doc_count=1,
        )
        assert True

    @staticmethod
    def test_integration_standard_scale() -> None:
        """Test integration with a standard scale."""
        # === Arrange, Act, & Assert
        InverseDocumentFrequencyCalculator(
            documents_with_term=1,
            documents_in_corpus=1,
            max_doc_count=1,
            scale=IdfScale.STANDARD,
        )
        assert True

    @staticmethod
    @pytest.mark.parametrize("bad_documents_with_term", [0, -1, -0.2, 3.2, 1.0])
    def test_bad_documents_with_term(bad_documents_with_term) -> None:
        """Raise value error when documents_with_term is not a positive integer."""
        # === Arrange, Act, & Assert
        with pytest.raises(ValueError, match=".*must be a positive integer.*"):
            InverseDocumentFrequencyCalculator(
                documents_with_term=bad_documents_with_term,
                documents_in_corpus=1,
                max_doc_count=1,
            )

    @staticmethod
    @pytest.mark.parametrize("bad_documents_in_corpus", [0, -1, -0.2, 3.2, 1.0])
    def test_bad_terms_in_document(bad_documents_in_corpus) -> None:
        """Raise value error when documents_in_corpus is not a positive integer."""
        # === Arrange, Act, & Assert
        with pytest.raises(ValueError, match=".*must be a positive integer.*"):
            InverseDocumentFrequencyCalculator(
                documents_with_term=1,
                documents_in_corpus=bad_documents_in_corpus,
                max_doc_count=1,
            )

    @staticmethod
    @pytest.mark.parametrize("bad_max_doc_count", [0, -1, -0.2, 3.2, 1.0])
    def test_bad_max_term_count(bad_max_doc_count) -> None:
        """Raise value error when max_doc_count is not a positive integer."""
        # === Arrange, Act, & Assert
        with pytest.raises(ValueError, match=".*must be a positive integer.*"):
            InverseDocumentFrequencyCalculator(
                documents_with_term=1,
                documents_in_corpus=1,
                max_doc_count=bad_max_doc_count,
            )
