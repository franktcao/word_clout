import time
from typing import List

import bs4
import requests
from bs4 import BeautifulSoup


class IndeedEntry:

    _url_description_base = "https://www.indeed.com/viewjob?jk="

    def __init__(self, entry):
        self.entry: bs4.element.Tag = entry
        self._location = "DEFAULT LOCATION"

    @property
    def link(self) -> str:
        return self.entry["data-jk"]

    @property
    def job_page(self) -> str:
        return self._url_description_base + self.link

    @property
    def job_title(self) -> str:
        job_title_container = self.entry.find(
            name="a", attrs={"data-tn-element": "jobTitle"},
        )
        return job_title_container.text.strip()

    # Not covered: protected method
    @property
    def _company_info(self) -> bs4.element.Tag:  # pragma: no cover
        return self.entry.find(class_="sjcl")

    # Not covered: protected method
    @property
    def _company_location_info(self):  # pragma: no cover
        return self._company_info.find(class_="location")

    @property
    def company_name(self) -> str:
        return self._company_info.find(name="company").text.strip()

    @property
    def location(self) -> str:
        # self._location = self._company_location_info.text.strip()
        if self._location == "DEFAULT LOCATION":
            return self._company_location_info.text.strip()
        else:
            return self._location

    @location.setter
    def location(self, location: str) -> None:
        self._location = location

    @property
    def neighborhood(self) -> str:
        # Extract neighborhood info if it"s there
        neighborhood_info = self._company_location_info.find(class_="span")
        neighborhood = neighborhood_info.text if neighborhood_info else ""

        self.location = self.location.rstrip(f"({neighborhood})")

        return neighborhood

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

    @_company_location_info.setter
    def _company_location_info(self, value):
        self.__company_location_info = value


class IndeedParser:
    url_base = "https://www.indeed.com/jobs?"

    def __init__(self, *, job_query: str, location: str) -> None:
        self.job_query = job_query
        self.location = location

    @property
    def full_url(self) -> str:
        job_query = f"q={self.job_query}"
        location = f"l={self.location.replace(',', '%2C')}"
        components = [self.url_base, job_query, location]
        return "&".join(components).replace(" ", "%20")

    def get_entries(self, **kwargs: str) -> List[IndeedEntry]:
        page = requests.get(self.full_url)
        parsed_page = BeautifulSoup(markup=page.text, parser="lxml")
        kwargs = {**dict(name="div", class_="row"), **kwargs}
        entries = parsed_page.find_all(**kwargs)
        return [IndeedEntry(entry) for entry in entries]
