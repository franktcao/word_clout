from typing import Any, List, Tuple
import requests
import bs4
from bs4 import BeautifulSoup
import time


class IndeedEntry:
    def __init__(self, entry):
        self.entry: bs4.element.Tag = entry

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
    def company_name(self) -> str:
        return self._company_info.find("company").text.strip()

    @property
    def _company_location_info(self):
        return self._company_info.find(class_="location")

    @property
    def location(self) -> str:
        return self._company_location_info.text.strip()

    @location.setter
    def location(self, location):
        self.location = location

    @property
    def link(self) -> str:
        return self.entry["data-jk"]

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

    def get_job_description(self, job_page) -> str:
        page = requests.get(job_page)
        time.sleep(1)  # ensuring at least 1 second between page grabs
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


    # # Loop over cities
    # for city_targ in city_set:
    #     URL_location = '&l=' + city_targ
    #     # Loop over pages
    #     for page_number in range(0, postings_per_city, POSTINGS_PER_PAGE):
    #         URL_page_start = '&start=' + str(page_number)
    #         URL = URL_base + URL_location + URL_page_start
    #
    #         page = requests.get(URL)
    #         time.sleep(1)  # ensuring at least 1 second between page grabs
    #         soup = BeautifulSoup(page.text, 'lxml')
    #
    #         # Loop over posts/entries
    #         entries = get_entries(soup)
    #         for i, entry in enumerate(entries):
    #             sys.stdout.write(
    #                 '\r' + ' page: ' + str(page_number // POSTINGS_PER_PAGE)
    #                 + ' / ' + str(max_pages_per_city)
    #                 + ', job posting: ' + str(i) + ' / ' + str(len(entries))
    #                 )
    #             title = get_job_title(entry)
    #             company = get_company(entry)
    #             #             city, state, zipcode, neighborhood = get_location_info(entry)
    #             location, neighborhood = get_location_info(entry)
    #             salary = get_salary(entry)
    #             link = get_link(entry)
    #
    #             # summary = get_job_summary(entry)
    #
    #             job_page = 'https://www.indeed.com/viewjob?jk=' + link
    #             #             print(link)
    #             description = get_job_description(job_page)
    #
    #             # Append the new row with data scraped
    #             num = (len(df) + 1)
    #             df.loc[num] = [title, company, location, neighborhood, description,
    #                            salary, link]