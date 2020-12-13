import time
from typing import List

import bs4
import requests
from bs4 import BeautifulSoup


class IndeedEntry:
    """Wrapper to help parse a job posting entry from an Indeed search results page."""

    _url_description_base = "https://www.indeed.com/viewjob?jk="

    def __init__(self, entry: bs4.element.Tag):
        """
        Initiate an entry and default location.

        :param entry:
        """
        self.entry = entry
        self._location = "DEFAULT LOCATION"

    @property
    def link(self) -> str:
        """Unique identifier of an entry to link to job page with description."""
        return self.entry["data-jk"]

    @property
    def job_page(self) -> str:
        """Page with job description."""
        return self._url_description_base + self.link

    @property
    def job_title(self) -> str:
        """Return job title of the job posting."""
        job_title_container = self.entry.find(
            name="a", attrs={"data-tn-element": "jobTitle"},
        )
        return job_title_container.text.strip()

    # Not covered: protected method
    @property
    def _company_info(self) -> bs4.element.Tag:  # pragma: no cover
        """Return container that holds company information."""
        return self.entry.find(class_="sjcl")

    # Not covered: protected method
    @property
    def _company_location_info(self):  # pragma: no cover
        """Return container that holds company location information."""
        return self._company_info.find(class_="location")

    @property
    def company_name(self) -> str:
        """Return company name."""
        return self.entry.find(class_="company").text.strip()

    @property
    def location(self) -> str:
        """Return job location.:w"""
        if self._location == "DEFAULT LOCATION":
            return self._company_location_info.text.strip()
        else:
            return self._location

    @location.setter
    def location(self, location: str) -> None:
        """Set location value."""
        self._location = location

    @property
    def neighborhood(self) -> str:
        """Return neighborhood from location if provided."""
        # Extract neighborhood info if it"s there
        neighborhood_info = self._company_location_info.find(name="span")
        neighborhood = neighborhood_info.text if neighborhood_info else ""

        self.location = self.location.rstrip(f"({neighborhood})")

        return neighborhood.strip("()")

    @property
    def salary(self) -> str:
        """Return salary from if provided."""
        try:
            salary_snippet = self.entry.find(name="div", class_="salarySnippet")
            salary_info = salary_snippet.find(name="span", class_="salary")
            return salary_info.text.strip()
        except AttributeError:
            return ""

    def get_job_summary(self) -> str:
        """Return job summary from search results page."""
        return self.entry.find(class_="summary").text.strip()

    def get_job_description(self) -> str:
        """Return full job description."""
        time.sleep(1)  # Ensuring at least 1 second between page grabs

        page = requests.get(self.job_page)
        soup = BeautifulSoup(page.text, features="lxml")
        description_info = soup.find(name="div", class_="jobsearch-jobDescriptionText")

        description = description_info.text.strip()
        description = description.replace("\n", " ")
        description = description.replace("\t", " ")

        return description


class IndeedParser:
    """Make an Indeed job search query and parse search results pages."""

    url_base = "https://www.indeed.com/jobs?"

    def __init__(self, *, job_query: str, location: str, desired_n_entries=10) -> None:
        """
        Initiate the parser with a query to search on Indeed.

        :param job_query:
            Query for job postings (e.g. "Data scientist", "software engineer", etc)
        :param location:
            Location for the job search (e.g. "Boston, MA", "New York", etc)
        :param desired_n_entries:
            Number of job postings desired to parse
        """

        self.job_query = job_query
        self.location = location
        self.n_entries = desired_n_entries

    @property
    def full_url(self) -> str:
        """Return full base URL with job query and location."""
        job_query = f"q={self.job_query}"
        location = f"l={self.location.replace(',', '%2C')}"
        components = [self.url_base, job_query, location]
        return "&".join(components).replace(" ", "%20")

    def get_page_url(self, page_number: int):
        """Return page URL including search results page number."""
        return (
            self.full_url + f"&start={page_number}"
            if page_number > 0
            else self.full_url
        )

    def get_entries(self, **kwargs: str) -> List[IndeedEntry]:
        """
        Return collection of wrapped job posting entries.

        :param kwargs:
            Keyword arguments for `find_all` to collect Indeed job posting entries
        :return:
            Collection of `IndeedEntry`s to help parse Indeed job postings
        """
        entries_per_page = 10
        n_pages = max(self.n_entries // entries_per_page, 1)
        entries = []
        for page_i in range(n_pages):
            page_url = self.get_page_url(entries_per_page * page_i)
            page = requests.get(page_url)
            parsed_page = BeautifulSoup(markup=page.text, features="lxml")
            kwargs = {**dict(name="div", class_="row"), **kwargs}
            entries += parsed_page.find_all(**kwargs)
        return [IndeedEntry(entry) for entry in entries]
