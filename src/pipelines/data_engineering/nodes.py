"""Get job postings from Indeed."""
from typing import List, Optional

import pandas as pd

from ...parsers import IndeedParser


def get_job_postings(
    job_query: str = "Data Scientist",
    locations: Optional[List[str]] = None,
    entries_per_location=100,
) -> pd.DataFrame:
    """
    Make a job search query for multiple locations and return dataframe of parsed job
    search results.

    :param job_query:
        Job search query (e.g. "Data scientist", "senior software engineer", etc)
    :param locations:
        Collection of cities to make the job search query for
    :param entries_per_location:
        Desired number of entries per location
    :return:
        Dataframe with job title, company, and salary information
    """
    locations = locations if locations else ["Boston, MA"]
    df = pd.DataFrame(
        # fmt: off
        columns=["link", "job_title", "company_name", "location", "salary", "description"]
        # fmt: on
    )

    # Loop over locations
    for location in locations:
        parser = IndeedParser(
            job_query=job_query,
            location=location,
            desired_n_entries=entries_per_location,
        )
        entries = parser.get_entries()
        df_location = pd.DataFrame(columns=df.columns)
        for i, entry in enumerate(entries):
            # Extract values
            link = entry.link
            job_title = entry.job_title
            company_name = entry.company_name
            location = entry.location
            salary = entry.salary
            description = entry.get_job_description()

            # Append row
            # fmt: off
            df_location.loc[i] = [link, job_title, company_name, location, salary, description]
            # fmt: on
        df = pd.concat([df, df_location], ignore_index=True)

    return df
