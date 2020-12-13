import pandas as pd

from src.parsers import IndeedEntry
from src.pipelines.data_engineering.nodes import get_job_postings


class TestGetJobPostings:
    @staticmethod
    def test_typical(mocker):
        """Assert the typical case where a query is tested for multiple locations."""
        # === Arrange
        query_to_test = "Data Scientist"
        locations_to_test = ["Boston, MA", "New York", "Europe"]

        expected = pd.DataFrame(
            {
                "link": 30 * ["some_link"],
                "job_title": 30 * ["some_title"],
                "company_name": 30 * ["some_company"],
                "location": 30 * ["some_location"],
                "salary": 30 * ["some_salary"],
                "description": 30 * ["some_description"],
            }
        )

        # Mock out entry returned by `IndeedParser.get_entries()`
        mocked_entry_instance = mocker.MagicMock(IndeedEntry)
        mocked_entry_instance.link = "some_link"
        mocked_entry_instance.job_title = "some_title"
        mocked_entry_instance.company_name = "some_company"
        mocked_entry_instance.location = "some_location"
        mocked_entry_instance.salary = "some_salary"
        mocked_entry_instance.get_job_description.return_value = "some_description"

        # Patch the parser
        mocked_parser_instance = mocker.MagicMock()
        mocked_parser_instance.get_entries.return_value = 10 * [mocked_entry_instance]
        mocked_parser = mocker.patch(
            "src.pipelines.data_engineering.nodes.IndeedParser",
            return_value=mocked_parser_instance,
        )

        # === Act
        actual = get_job_postings(
            job_query=query_to_test, locations=locations_to_test, entries_per_location=1
        )

        # === Assert
        # Check that it reaches the last location
        mocked_parser.assert_called_with(
            job_query="Data Scientist", location="Europe", desired_n_entries=1
        )
        # Check the actual dataframe is returned as expected
        pd.testing.assert_frame_equal(actual, expected, check_like=True)
