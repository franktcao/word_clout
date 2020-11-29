import pytest
from bs4 import BeautifulSoup

from src.parsers import IndeedParser, IndeedEntry


class TestIndeedParser:
    @pytest.fixture
    def object_under_test(self):
        """Instantiate object to test."""
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


class TestIndeedEntry:
    @pytest.fixture
    def object_under_test(self, mocker) -> IndeedEntry:
        mocked_soup = mocker.MagicMock(BeautifulSoup)
        _object_under_test = IndeedEntry(mocked_soup)

        return _object_under_test

    def test_link(self, object_under_test: IndeedEntry):
        # === Arrange
        expected = "some_link"

        mocked_entry = {"data-jk": "some_link"}
        object_under_test.entry = mocked_entry

        # === Act
        actual = object_under_test.link

        # === Assert
        assert actual == expected

    def test_job_page(self, object_under_test: IndeedEntry):
        # === Arrange
        expected = "https://www.indeed.com/viewjob?jk=some_link"

        mocked_entry = {"data-jk": "some_link"}
        object_under_test.entry = mocked_entry

        # === Act
        actual = object_under_test.job_page

        # === Assert
        assert actual == expected

    def test_job_title(self, mocker, object_under_test: IndeedEntry):
        # === Arrange
        expected = "Data Scientist"

        # Need an element with a text attribute that wraps job title
        mocked_bs4_element = mocker.MagicMock()
        mocked_bs4_element.text = "Data Scientist"

        # Return element that wraps job title when `find` is called
        mocked_entry = mocker.MagicMock(BeautifulSoup)
        mocked_entry.find = lambda name, attrs: mocked_bs4_element

        object_under_test.entry = mocked_entry

        # === Act
        actual = object_under_test.job_title

        # === Assert
        assert actual == expected

    def test_company_name(self, mocker, object_under_test: IndeedEntry):
        # === Arrange
        expected = "Aperture"

        # Wrap company name
        mocked_company_name = mocker.MagicMock()
        mocked_company_name.text = "Aperture"

        # Wrap mocked company location info
        mocked_company_location_info = mocker.MagicMock()
        mocked_company_location_info.find = lambda company: mocked_company_name

        # Construct object to test that contains wrapped objects
        mocked_entry = mocker.MagicMock(BeautifulSoup)
        mocked_entry.find = lambda class_: mocked_company_location_info
        object_under_test = IndeedEntry(mocked_entry)

        # === Act
        actual = object_under_test.company_name

        # === Assert
        assert actual == expected
