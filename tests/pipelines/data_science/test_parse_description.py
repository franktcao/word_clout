from pathlib import Path
from typing import cast
from unittest.mock import call

import pandas as pd

from pyspark.shell import sqlContext
from src.pipelines.data_science.parse_description import (
    append_document_appearances,
    append_total_and_max_counts_in_doc,
    clean_description,
    convert_descriptions_to_term_counts,
    count_terms,
    write_term_counts_to_parquet,
)


class TestConvertDescriptionStats:
    @staticmethod
    def test_simple_case(mocker, tmp_path):
        """Assert convert_descriptions_to_term_counts inputs passed in as expected."""
        # === Arrange
        mocked_df = mocker.MagicMock()

        # Mocked since unit tested elsewhere
        module_prefix = "src.pipelines.data_science.parse_description"
        mocked_inner_function = mocker.patch(
            f"{module_prefix}.write_term_counts_to_parquet",
        )
        # Mocked since no parquet file is actually written
        mocked_returned_df = mocker.MagicMock()
        mocked_spark_data = mocker.patch(f"{module_prefix}.sqlContext")
        mocked_spark_data.read.parquet.return_value = mocked_returned_df

        # Mocked since unit tested elsewhere
        mocked_append_total_and_max_counts_in_doc = mocker.patch(
            f"{module_prefix}.append_total_and_max_counts_in_doc"
        )

        # === Act
        convert_descriptions_to_term_counts(data=mocked_df, intermediate_path=tmp_path)

        # === Assert
        mocked_inner_function.assert_called_with(
            mocked_df, output_directory=Path(tmp_path)
        )
        mocked_spark_data.read.parquet.assert_called_once_with(str(tmp_path))
        mocked_append_total_and_max_counts_in_doc.assert_called_with(mocked_returned_df)


class TestAppendTotalAndMaxCountsInDoc:
    @staticmethod
    def test_expected_counts():
        """Assert counts and maximums are extracted as expected."""
        # === Arrange
        data_to_test = sqlContext.createDataFrame(
            pd.DataFrame(
                {
                    # Used to help compare
                    "uid": range(10),
                    "corpus_id": 3 * ["1"] + 5 * ["2"] + 2 * ["3"],
                    "count_in_document": [1, 2, 3] + [4, 5, 6, 7, 8] + [9, 10],
                }
            )
        )
        expected = pd.DataFrame(
            {
                "uid": range(10),
                "corpus_id": 3 * ["1"] + 5 * ["2"] + 2 * ["3"],
                "count_in_document": [1, 2, 3] + [4, 5, 6, 7, 8] + [9, 10],
                "total_terms_in_document": 3 * [6] + 5 * [30] + 2 * [19],
                "max_count_in_document": 3 * [3] + 5 * [8] + 2 * [10],
            }
        )

        # === Act
        actual = (
            append_total_and_max_counts_in_doc(df=data_to_test)
            .toPandas()
            .sort_values("uid")
            .reset_index(drop=True)
        )

        # === Assert
        pd.testing.assert_frame_equal(actual, expected)


class TestExtractDescriptionStats:
    @staticmethod
    def test_table_constructed_and_saved_as_expected(mocker, tmp_path):
        """Assert description stats table is constructed and saved as expected."""
        # === Arrange
        data_to_test = pd.DataFrame(
            {
                "link": [1, 2],
                "description": ["some description", "some other description"],
            }
        )
        mocked_clean_description = mocker.patch(
            "src.pipelines.data_science.parse_description.clean_description",
            side_effect=lambda x: x,
        )
        mocked_returned_df = mocker.MagicMock()
        mocked_count_terms = mocker.patch(
            "src.pipelines.data_science.parse_description.count_terms",
            side_effect=lambda text: mocked_returned_df,
        )

        # === Act
        write_term_counts_to_parquet(data=data_to_test, output_directory=tmp_path)

        # === Assert
        # Assert intermediate functions are called as expected
        mocked_clean_description.assert_has_calls(
            [call("some description"), call("some other description")]
        )
        # Because this is called with kwargs, only the last call can be checked against
        mocked_count_terms.assert_called_with(text="some other description")

        # Assert the corpus ID is recorded for every row
        mocked_returned_df.loc.__setitem__.assert_has_calls(
            [call((slice(None), "corpus_id"), 2)]
        )

        # Assert parquet is written for each row
        mocked_returned_df.to_parquet.assert_has_calls(
            [call(tmp_path / "1.parquet"), call(tmp_path / "2.parquet")]
        )


class TestCountTerms:
    @staticmethod
    def test_typical_case():
        """Assert returned dataframe counts terms as expected."""
        # === Arrange
        raw_data = 1 * ["one"] + 2 * ["two"] + 3 * ["three"] + 4 * ["four"]
        text_to_test = " ".join(raw_data)

        expected = pd.DataFrame(
            {"term": ["one", "two", "three", "four"], "count_in_document": [1, 2, 3, 4]}
        )

        # === Act
        actual = count_terms(text=text_to_test)

        # === Assert
        pd.testing.assert_frame_equal(actual, expected)


class TestCleanDescription:
    @staticmethod
    def test_typical_case():
        """Assert text is cleaned as expected when certain chars are passed in."""
        # === Arrange
        text_to_test = "This is \n some \t text."
        expected = "this is   some   text."

        # === Act
        actual = clean_description(text=text_to_test)

        # === Assert
        assert actual == expected

    @staticmethod
    def test_lower_cased():
        """Assert text is transformed to lower case characters."""
        # === Arrange
        text_to_test = "ThIs Is sOMe tExT."
        expected = "this is some text."

        # === Act
        actual = clean_description(text=text_to_test)

        # === Assert
        assert actual == expected

    @staticmethod
    def test_remove_escape_chars():
        """Assert text is transformed to remove escape characters."""
        # === Arrange
        text_to_test = "\n\n\n\t\n\n"
        expected = "      "

        # === Act
        actual = clean_description(text=text_to_test)

        # === Assert
        assert actual == expected


class TestAppendDocumentAppearances:
    @staticmethod
    def test_appending_document_appearances_column():
        """Assert columns append correct calculations."""
        # === Arrange
        raw_data = pd.DataFrame(
            {
                # Here common_word is in both documents "1" and "2" but others are not
                "term": ["common_word", "word_1", "common_word", "word_2"],
                "corpus_id": ["1", "1", "2", "2"],
            }
        )
        df_to_test = sqlContext.createDataFrame(raw_data)

        expected = raw_data
        expected["document_appearances"] = [2, 1, 2, 1]
        expected = expected.sort_values("term").reset_index(drop=True)

        # === Act
        actual = (
            cast(pd.DataFrame, append_document_appearances(df_to_test).toPandas())
            .sort_values("term")
            .reset_index(drop=True)
        )

        # === Assert
        pd.testing.assert_frame_equal(actual, expected)
