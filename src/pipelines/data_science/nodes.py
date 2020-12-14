from typing import List
import pandas as pd


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    result = expand_location(df)
    return result


def expand_location(df: pd.DataFrame) -> pd.DataFrame:
    df["city"] = df["location"].apply()
    df["state"] = df["location"].apply()
    df["neighborhood"] = df["location"].apply()
    df["zip_code"] = df["location"].apply()
    return df.drop("location", axis=1)


def parse_location(location: str) -> List[str]:
    """Parse location information into city, state, zip code, and neighborhood.

    :param location:
        Location that contains city, state, zip code, and neighborhood.
        *Note*: Zip code and neighborhood may not be present.
    :return:
        City, state, zip code, and neighborhood
    """
    location_split = location.split()
    city = location_split[0].replace(",", "").strip()
    state = location_split[1]
    zip_code = None
    neighborhood = None

    # If there happens to be extra location information, parse it into zip code and
    # neighborhood.
    extra_location_info = location_split[2:] if len(location_split) > 2 else None
    if extra_location_info:
        # Check to see if the zip code is first entry or last in extra info
        if _is_zip_code(extra_location_info[0]):
            zip_code = extra_location_info[0]
        elif _is_zip_code(extra_location_info[-1]):
            zip_code = extra_location_info[-1]
        neighborhood = (
            " ".join(filter(lambda x: x != zip_code, extra_location_info))
            .strip("()")
            .replace("area", "")
            .strip()
            if len(extra_location_info) > 1
            else None
        )

    return [city, state, zip_code, neighborhood]


def _is_zip_code(token: str):
    """Return whether token is zip code or not.

    :param token:
        Candidate zip code
    :return:
        True if token is zip code, otherwise False
    """
    less_than_5_digits = len([char for char in token if char.isdigit()]) < 5
    return not less_than_5_digits
