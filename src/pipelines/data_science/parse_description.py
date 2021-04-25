from collections import Counter
from pathlib import Path
from typing import List

import pandas as pd
from tqdm import tqdm

import pyspark.sql.functions as F
from pyspark.shell import sqlContext
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.window import Window
from src.definitions import TERM_FREQ_DIR


def convert_descriptions_to_term_counts(
    data: pd.DataFrame, intermediate_path: Path = TERM_FREQ_DIR
) -> SparkDataFrame:
    """Convert job descriptions to term counts.

    :param data:
        Table with job description columns
    :param intermediate_path:
        Path to write intermediate dataset to
    :return:
        Table with term frequency counts
    """
    write_term_counts_to_parquet(data, output_directory=intermediate_path)

    df = sqlContext.read.load(str(intermediate_path))

    return append_total_and_max_counts_in_doc(df)


def append_total_and_max_counts_in_doc(df: SparkDataFrame) -> SparkDataFrame:
    """
    Append a column for total number of terms in document and a column for maximum
    count in document.

    :param df:
        Table with term counts for each 'corpus_id'. Required columns:
            * count_in_document
            * corpus_id`
    :return:
        `df` with additional columns:
            * total_terms_in_document
            * max_count_in_document
    """
    _validate_has_columns(df, ["count_in_document", "corpus_id"])

    window = Window.partitionBy("corpus_id")
    result = df.withColumn(
        "total_terms_in_document", F.sum(F.col("count_in_document")).over(window)
    ).withColumn(
        "max_count_in_document", F.max(F.col("count_in_document")).over(window)
    )
    return result


def write_term_counts_to_parquet(data: pd.DataFrame, output_directory: Path) -> None:
    """
    Convert text descriptions in each row of `data` to its own table. Each row of the
    resulting table has a term, its frequency counts in the description, and the ID
    where that description comes from.

    :param data:
        Table where each row has a job description and unique ID
    :param output_directory:
        Directory to write description stat table to
    """
    _validate_has_columns(data, ["link", "description"])
    for row in tqdm(data.to_dict(orient="records"), desc="Row"):
        uid = row["link"]
        description = row["description"]

        description = clean_description(description)

        df = count_terms(text=description)
        df.loc[:, "corpus_id"] = uid

        df.to_parquet(output_directory / f"{uid}.parquet")


def count_terms(text: str) -> pd.DataFrame:
    """Tokenize, count, and return table with term counts.

    :param text:
        Words to tokenize and count
    :return:
        Table with columns for individual terms and counts
    """
    term_counts = Counter(text.split())
    df = pd.DataFrame(term_counts.items(), columns=["term", "count_in_document"])

    return df


def clean_description(text: str) -> str:
    """Clean description of characters to improve tokenization.

    :param text:
        Description text to clean
    :return:
        Cleaned description text
    """
    text = text.replace("\n", " ")
    text = text.replace("\t", " ")
    text = text.lower()

    return text


def append_document_appearances(df: SparkDataFrame) -> SparkDataFrame:
    """Append column to `df` to count how many documents contain each term.

    :param df:
        Table with terms and corpus unique ID to extract document frequency.
        Requires columns:
            * term
            * corpus_id
    :return:
        Original table `df` with extra column:
            * document_appearances
    """
    _validate_has_columns(df, ["term", "corpus_id"])
    window = Window().partitionBy("term")

    return df.withColumn("document_appearances", F.count("corpus_id").over(window))


# Not covered: Validation
def _validate_has_columns(
    df: SparkDataFrame, required_columns: List[str]
) -> None:  # pragma: no cover
    if not set(required_columns).issubset(df.columns):
        raise ValueError(
            "`df` does not contain required columns."
            f"\n\tMissing `df` columns: {set(required_columns).difference(df.columns)}"
            f"\n\t`df` columns: {df.columns}"
            f"\n\tRequired columns: {required_columns}"
        )


# if __name__ == "__main__":
#     append_idf()
