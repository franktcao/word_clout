import pytest
from src.parsers import IndeedParser, IndeedEntry


class TestIndeedParser:
    @pytest.fixture
    def object_under_test(self):
        return IndeedParser(job_query="Data Scientist", location="Boston, MA")

    def test_full_url(self, object_under_test: IndeedParser):
        """Assert that full URL is constructed as expected."""
        # === Arrange
        base_url = "https://www.indeed.com/jobs?"
        job_component = "&q=Data%20Scientist"
        location_component = "&l=Boston%2C%20MA"
        expected = base_url + job_component + location_component

        # === Act (done in fixture)

        # === Assert
        actual = object_under_test.full_url
        assert actual == expected

    def test_get_entries_count(self, mocker, object_under_test: IndeedParser):
        """Assert getting entries returns correct number of entries."""
        # === Arrange
        mocked_request = mocker.MagicMock()
        mocked_request.text = 15 * "<div class=row>"
        mocker.patch("src.parsers.requests.get", return_value=mocked_request)

        # === Act
        actual = object_under_test.get_entries()

        # === Assert
        assert len(actual) == 15


