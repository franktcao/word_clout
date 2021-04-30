import pytest
from pytest_steps import test_steps

from src.pipelines.data_science.append_tfidf import (  # get_term_frequency,
    get_document_frequency,
    get_inverse_doc_frequency,
)


class TestGetDocumentFrequency:
    @staticmethod
    @test_steps("standard", "smooth", "max", "prob")
    def test_different_methods_pass_through(mocker) -> None:
        """Assert different methods call expected functions."""
        # === Arrange
        expected = 1

        doc_count_to_test = 123
        n_posts_to_test = 456

        mocked_implementation = mocker.patch(
            "src.pipelines.data_science.append_tfidf._doc_freq_standard", return_value=1
        )

        # === Act
        actual = get_document_frequency(
            doc_count=doc_count_to_test, n_posts=n_posts_to_test, method="standard",
        )

        # === Assert
        assert actual == expected
        mocked_implementation.assert_called_with(doc_count_to_test, n_posts_to_test)
        yield

        # === Arrange
        expected = 2
        mocked_implementation = mocker.patch(
            "src.pipelines.data_science.append_tfidf._doc_freq_smooth", return_value=2
        )

        # === Act
        actual = get_document_frequency(
            doc_count=doc_count_to_test, n_posts=n_posts_to_test, method="smooth",
        )

        # === Assert
        assert actual == expected
        mocked_implementation.assert_called_with(doc_count_to_test, n_posts_to_test)
        yield

        # === Arrange
        expected = 3
        mocked_implementation = mocker.patch(
            "src.pipelines.data_science.append_tfidf._doc_freq_max", return_value=3
        )

        # === Act
        actual = get_document_frequency(
            doc_count=doc_count_to_test,
            n_posts=n_posts_to_test,
            method="max",
            max_doc_count=3,
        )

        # === Assert
        assert actual == expected
        mocked_implementation.assert_called_with(doc_count_to_test, max_doc_count=3)
        yield

        # === Arrange
        expected = 4
        mocked_implementation = mocker.patch(
            "src.pipelines.data_science.append_tfidf._doc_freq_prob", return_value=4
        )

        # === Act
        actual = get_document_frequency(
            doc_count=doc_count_to_test, n_posts=n_posts_to_test, method="prob",
        )

        # === Assert
        assert actual == expected
        mocked_implementation.assert_called_with(doc_count_to_test, n_posts_to_test)
        yield

    @staticmethod
    def test_bad_method() -> None:
        """Raise error when method is not recognized."""
        # === Arrange, Act, & Assert
        with pytest.raises(ValueError, match=r".*method.* must be one of"):
            get_document_frequency(doc_count=1, n_posts=1, method="whatev")


class TestGetInverseDocumentFrequency:
    @staticmethod
    @test_steps(None, "log")
    def test_rescale_methods(mocker) -> None:
        """Assert logic flows as expected for different rescale methods."""
        # === Arrange
        expected = 1

        # === Act
        actual = get_inverse_doc_frequency(doc_freq=1)

        # === Assert
        assert actual == expected
        yield

        # === Arrange
        expected = 2
        mocked_implementation = mocker.patch(
            "src.pipelines.data_science.append_tfidf.math.log", return_value=2
        )

        # === Act
        actual = get_inverse_doc_frequency(doc_freq=2, rescale_method="log")

        # === Assert
        assert actual == expected
        mocked_implementation.assert_called_with(1 / 2)
        yield

    @staticmethod
    def test_bad_method() -> None:
        """Raise error when rescale method is not recognized."""
        # === Arrange, Act, & Assert
        with pytest.raises(ValueError, match=r".*method.* must be one of"):
            get_inverse_doc_frequency(doc_freq=2, rescale_method="whatever")
