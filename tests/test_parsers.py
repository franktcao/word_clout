import pytest
from pytest_steps import test_steps

from bs4 import BeautifulSoup

from src.parsers import IndeedParser, IndeedEntry


class TestIndeedParser:
    @staticmethod
    @pytest.fixture
    def object_under_test():
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
    @staticmethod
    @pytest.fixture
    def object_under_test(mocker) -> IndeedEntry:
        mocked_soup = mocker.MagicMock(BeautifulSoup)
        _object_under_test = IndeedEntry(mocked_soup)

        return _object_under_test

    @staticmethod
    @pytest.fixture
    def object_under_test_simple_entry(object_under_test) -> IndeedEntry:
        mocked_entry = {"data-jk": "some_link"}
        object_under_test.entry = mocked_entry

        return object_under_test

    def test_link(self, object_under_test_simple_entry: IndeedEntry):
        # === Arrange
        expected = "some_link"

        # === Act
        actual = object_under_test_simple_entry.link

        # === Assert
        assert actual == expected

    def test_job_page(self, object_under_test_simple_entry: IndeedEntry):
        # === Arrange
        expected = "https://www.indeed.com/viewjob?jk=some_link"

        # === Act
        actual = object_under_test_simple_entry.job_page

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

    @staticmethod
    @pytest.fixture
    def object_under_test_company_info(
        mocker, object_under_test: IndeedEntry
    ) -> IndeedEntry:
        # Wrap company name
        mocked_company_name = mocker.MagicMock()
        mocked_company_name.text = "Aperture"

        mocked_location = mocker.MagicMock()
        mocked_location.text = "Boston, MA (South End)"
        mocked_neighborhood = mocker.MagicMock()
        mocked_neighborhood.text = "South End"
        mocked_location.find = lambda class_: mocked_neighborhood

        mocked_location.class_ = mocked_neighborhood

        container = {"name": mocked_company_name, "class_": mocked_location}
        # Wrap mocked company location info
        mocked_company_info = mocker.MagicMock()
        mocked_company_info.find = lambda **keys: container[
            [key for key, val in keys.items()][0]
        ]

        # Construct object to test that contains wrapped objects
        mocked_entry = mocker.MagicMock(BeautifulSoup)
        mocked_entry.find = lambda class_: mocked_company_info
        object_under_test = IndeedEntry(mocked_entry)

        return object_under_test

    @staticmethod
    def test_company_name(object_under_test_company_info: IndeedEntry):
        # === Arrange
        expected = "Aperture"

        # === Act
        actual = object_under_test_company_info.company_name

        # === Assert
        assert actual == expected

    @staticmethod
    def test_location(object_under_test_company_info):
        # === Arrange
        expected = "Boston, MA (South End)"

        # === Act
        actual = object_under_test_company_info.location

        # === Assert
        assert actual == expected

    @staticmethod
    @test_steps("neighborhood", "updated_location")
    def test_neighborhood(object_under_test_company_info):
        # === Arrange
        expected_neighborhood = "South End"
        expected_location = "Boston, MA"

        # === Act
        actual_neighborhood = object_under_test_company_info.neighborhood
        actual_location = object_under_test_company_info.location

        # === Assert
        assert actual_neighborhood == expected_neighborhood
        yield

        assert actual_location == expected_location
        yield

    @staticmethod
    def test_salary(mocker):
        # === Arrange
        expected = "100,000"
        # Construct object to test that contains wrapped objects
        mocked_entry = mocker.MagicMock(BeautifulSoup)
        mocked_salary_snippet = mocker.MagicMock()
        mocked_entry.find = lambda name, class_: mocked_salary_snippet

        mocked_salary_info = mocker.MagicMock()
        mocked_salary_info.text = "100,000"
        mocked_salary_snippet.find = lambda name, class_: mocked_salary_info
        object_under_test = IndeedEntry(mocked_entry)

        # === Act
        actual = object_under_test.salary

        # === Assert
        assert actual == expected

    @staticmethod
    def test_no_salary(mocker):
        # === Arrange
        expected = ""
        # Construct object to test that contains wrapped objects
        mocked_entry = mocker.MagicMock(BeautifulSoup)
        mocked_entry.find = lambda name, class_: None

        object_under_test = IndeedEntry(mocked_entry)

        # === Act
        actual = object_under_test.salary

        # === Assert
        assert actual == expected

    @staticmethod
    def test_job_summary(mocker):
        # === Arrange
        summary = "This is a job summary of the job that you will be applying for."
        expected = summary

        # Construct object to test that contains wrapped objects
        mocked_entry = mocker.MagicMock(BeautifulSoup)
        mocked_summary_info = mocker.MagicMock()
        mocked_summary_info.text = expected
        mocked_entry.find = lambda class_: mocked_summary_info

        object_under_test = IndeedEntry(mocked_entry)

        # === Act
        actual = object_under_test.get_job_summary()

        # === Assert
        assert actual == expected

    @staticmethod
    def test_job_description(mocker):
        # === Arrange
        description = "This is the job description, a more thorough job summary."
        expected = description

        # Construct object to test that contains wrapped objects
        mocked_entry = mocker.MagicMock(BeautifulSoup)

        mocked_request = mocker.MagicMock()
        mocker.patch("src.parsers.requests.get", return_value=mocked_request)

        mocked_soup = mocker.MagicMock(BeautifulSoup)
        mocked_description_info = mocker.MagicMock()
        mocked_description_info.text = description
        mocked_soup.find = lambda name, class_: mocked_description_info
        mocker.patch("src.parsers.BeautifulSoup", return_value=mocked_soup)

        object_under_test = IndeedEntry(mocked_entry)

        # === Act
        actual = object_under_test.get_job_description()

        # === Assert
        assert actual == expected
