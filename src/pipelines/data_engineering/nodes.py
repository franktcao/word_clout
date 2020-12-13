# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
"""Example code for the nodes in the example pipeline. This code is meant
just for illustrating basic Kedro features.

PLEASE DELETE THIS FILE ONCE YOU START WORKING ON YOUR OWN PROJECT!
"""

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
