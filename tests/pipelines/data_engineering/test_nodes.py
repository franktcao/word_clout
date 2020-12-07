import pandas as pd
from src.pipelines.data_engineering.nodes import get_job_postings


class TestGetJobPostings:
    @staticmethod
    def test_typical(mocker):
        # === Arrange
        expected = pd.DataFrame()
        query_to_test = "Data Scientist"
        locations_to_test = ["Boston, MA", "New York", "Europe"]
        mocked_indeed = mocker.MagicMock()
        mocked_indeed.get_entries = 10 * ["hi"]
        mocked_parser = mocker.patch(
            "src.parsers.IndeedParser", return_value=mocked_indeed
        )
        mocked_entry = mocker.MagicMock()
        mocked_entry.link = "some_link"
        mocker.patch("src.parsers.IndeedEntry", return_value=mocked_entry)

        # === Act
        actual = get_job_postings(
            job_query=query_to_test, locations=locations_to_test, entries_per_location=1
        )

        # === Assert
        mocked_parser.assert_called_once()
        assert actual == expected
