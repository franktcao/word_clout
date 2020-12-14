from typing import List
import pandas as pd


# Not covered: Essentially a script over other unit tested functions
def clean_data(df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover
    """Parse location information to append city, state, zip code, and neighborhood
    columns.

    :param df:
        Dataframe with "location" column
    :return:
        Dataframe with "location" column dropped but expanded into other,
        finer-detailed columns
    """
    result = expand_location(df)
    return result


def expand_location(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace "location" column with "city", "state", "zip_code", and "neighborhood"
    columns extracted from it.

    :param df:
        Dataframe with "location" column
    :return:
        Dataframe with "location" column dropped and "city", "state", "zip_code" and
        "neighborhood" columns appended
    """
    # Append a column of the parsed location
    df["parsed_location_info"] = df.apply(
        func=lambda row: parse_location(row["location"]), axis=1
    )
    # Concatenate original columns with horizontal/column exploded column of arrays
    df = df.apply(
        func=lambda row: pd.concat(
            [row, pd.Series([e for e in row["parsed_location_info"]])], axis=0
        ),
        axis=1,
    )
    # Rename the default-named exploded columns
    df.rename(
        columns={0: "city", 1: "state", 2: "zip_code", 3: "neighborhood"}, inplace=True
    )

    return df.drop(["location", "parsed_location_info"], axis=1)


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
