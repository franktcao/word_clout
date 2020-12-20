from typing import List
import pandas as pd


# Not covered: Essentially a script over other unit tested functions
def clean_data(df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover
    """Parse
    * location information to append city, state, zip code, and neighborhood columns
    * salary information to append minimum and maximum annual salary in dollars

    :param df:
        Dataframe with "location" column
    :return:
        Dataframe with "location" column dropped but expanded into other,
        finer-detailed columns
    """
    result = expand_location(df)
    result = expand_salary(result)
    return result


def expand_salary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace "salary" column with "min_annual_salary_$" and "max_annual_salary_$" columns
    extracted from it.

    :param df:
        Dataframe with "salary" column
    :return:
        Dataframe with "salary" column dropped and "min_annual_salary_$" and
        "max_annual_salary_$" columns appended
    """
    original_cols = df.columns
    salary = "salary"
    replacement_columns = ["annual_salary_min_$", "annual_salary_max_$"]
    columns_to_expanded = {salary: replacement_columns}

    # Parse salary information into list
    df["parsed_salary"] = df[df[salary].notnull()].apply(
        func=lambda row: parse_salary(row[salary]), axis=1
    )
    # Explode (non-null) parsed list into its own columns
    df_expanded_salary = (
        df[["link", salary, "parsed_salary"]][df[salary].notnull()]
        .apply(
            func=lambda row: pd.concat(
                [row, pd.Series([e for e in row["parsed_salary"]])], axis=0
            ),
            axis=1,
        )
        .drop([salary, "parsed_salary"], axis=1)
        # Rename the default-named exploded columns
        .rename(columns={i: col for i, col in enumerate(replacement_columns)})
    )

    # Join non-null columns back in on unique identifier
    df = pd.merge(df, df_expanded_salary, how="outer", on="link")

    new_columns = replace_expanded_columns(original_cols, columns_to_expanded)
    return df[new_columns]


def replace_expanded_columns(original_cols, mapping):
    """Expand columns in `original_cols` which are in `mapping` with mapped values.

    :param original_cols:
        Original columns with subset of columns to expand
    :param mapping:
        Mapping from original columns to expanded columns
    :return:
        Expanded column names
    """
    new_columns = []
    for col in original_cols:
        if col in mapping:
            new_columns.extend(mapping[col])
        else:
            new_columns.append(col)
    return new_columns


def parse_salary(salary: str) -> List[float]:
    """Parse salary information into minimum and maximum annual salary in dollars.

    :param salary:
        Salary that contains a value or range and a rate
    :return:
        Minimum and maximum annual salary in dollars
    """
    salary_split = salary.split()
    splitter = [
        i for i, token in enumerate(salary_split) if token == "a" or token == "an"
    ].pop()
    range_ = " ".join(salary_split[:splitter])
    rate = " ".join(salary_split[splitter:])
    rate_to_annual = {
        "a year": 1,
        "an hour": 2_080,
    }
    range_split = range_.split("-")
    salary_min, salary_max = (
        range_split if len(range_split) > 1 else (range_split[0], range_split[0])
    )
    if "Up to" in salary_min:
        salary_min = "0"
        salary_max = salary_max.replace("Up to ", "")

    annual_rate = rate_to_annual[rate]
    annual_salary_min = float(_remove_human_readables(salary_min)) * annual_rate
    annual_salary_max = float(_remove_human_readables(salary_max)) * annual_rate
    return [annual_salary_min, annual_salary_max]


def _remove_human_readables(token: str) -> str:
    """Remove human readable characters from a string. Example: "$1,000" -> "1000"

    :param token:
        Token to remove human readable characters
    :return:
        Token without human readable characters (to cast to numeric type)
    """
    # Translate bad characters to empty ones
    translation = str.maketrans({"$": "", ",": ""})
    return token.translate(translation)


def expand_location(df: pd.DataFrame) -> pd.DataFrame:
    """Replace "location" column with "city", "state", "zip_code", and "neighborhood"
    columns extracted from it.

    :param df:
        Dataframe with "location" column
    :return:
        Dataframe with "location" column dropped and "city", "state", "zip_code" and
        "neighborhood" columns appended
    """
    original_cols = df.columns
    location = "location"
    replacement_columns = ["city", "state", "zip_code", "neighborhood"]
    columns_to_expanded = {location: replacement_columns}

    # Append a column of the parsed location
    df["parsed_location_info"] = df[df.columns].apply(
        func=lambda row: parse_location(row[location]), axis=1
    )

    # Explode parsed list into its own columns
    df = (
        df[df.columns].apply(
            func=lambda row: pd.concat(
                [row, pd.Series([e for e in row["parsed_location_info"]])], axis=0
            ),
            axis=1,
        )
        # Rename the default-named exploded columns
        .rename(columns={i: col for i, col in enumerate(replacement_columns)})
    )

    new_columns = replace_expanded_columns(original_cols, mapping=columns_to_expanded)
    return df[new_columns]


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
