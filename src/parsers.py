from typing import Any, List, Tuple
import requests
import bs4
from bs4 import BeautifulSoup
import time


class IndeedEntry:

    _url_description_base = "https://www.indeed.com/viewjob?jk="

    def __init__(self, entry):
        self.entry: bs4.element.Tag = entry

    @property
    def link(self) -> str:
        return self.entry["data-jk"]

    @property
    def job_page(self) -> str:
        return self._url_description_base + self.link

    @property
    def job_title(self) -> str:
        job_title_container = self.entry.find(
            name="a",
            attrs={"data-tn-element": "jobTitle"},
        )
        return job_title_container.text.strip()

    @property
    def _company_info(self) -> bs4.element.Tag:
        return self.entry.find(class_="sjcl")

    @property
    def _company_location_info(self):
        return self._company_info.find(class_="location")

    @property
    def company_name(self) -> str:
        return self._company_info.find("company").text.strip()

    @property
    def location(self) -> str:
        return self._company_location_info.text.strip()

    @location.setter
    def location(self, location):
        self.location = location

    @property
    def neighborhood(self) -> str:
        # Extract neighborhood info if it"s there
        neighborhood_info = self._company_location_info.find(class_="span")
        neighborhood = neighborhood_info.text if neighborhood_info else ""

        self.location = self.location.rstrip(neighborhood)

        return neighborhood.strip("()")

    @property
    def salary(self) -> str:
        try:
            return self.entry.find("nobr").text.strip()
        except AttributeError:
            try:
                salary_container = self.entry.find(name="div", class_="salarySnippet")
                salary_temp = salary_container.find(name="span", class_="salary")
                return salary_temp.text.strip()
            except AttributeError:
                return ""

    def get_job_summary(self) -> str:
        return self.entry.find(class_="summary").text.strip()

    def get_job_description(self) -> str:
        time.sleep(1)  # Ensuring at least 1 second between page grabs

        page = requests.get(self.job_page)
        soup = BeautifulSoup(page.text, "lxml")
        description = soup.find(name="div", class_="jobsearch-jobDescriptionText")

        description = description.text.strip()
        description = description.replace("\n", " ")
        description = description.replace("\t", " ")

        return description


class IndeedParser:
    url_base = "https://www.indeed.com/jobs?"

    def __init__(self, *, job_query: str, location: str) -> None:
        self.job_query = job_query
        self.location = location

    @property
    def full_url(self) -> str:
        job_query = f"q={self.job_query.lower()}"
        location = f"l={self.location.lower().replace(',', '%2C')}"
        components = [self.url_base, job_query, location]
        return "&".join(components).replace(" ", "%20")

    def get_entries(self, **kwargs: str) -> List[IndeedEntry]:
        page = requests.get(self.full_url)
        parsed_page = BeautifulSoup(markup=page.text, parser="lxml")
        kwargs = {**dict(name="div", class_="row"), **kwargs}
        entries = parsed_page.find_all(**kwargs)
        return [IndeedEntry(entry) for entry in entries]


