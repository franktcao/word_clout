from collections import Counter
from pathlib import Path

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
    """Convert job descriptions to term frequency counts.

    :param data:
        Table with job description columns
    :param intermediate_path:
        Path to write intermediate dataset to
    :return:
        Table with term frequency counts
    """
    write_term_counts_to_parquet(data, output_directory=intermediate_path)

    return sqlContext.read.load(str(intermediate_path))


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
    for row in tqdm(data.to_dict(orient="records"), desc="Row"):
        uid = row["link"]
        description = row["description"]

        description = clean_description(description)

        df = count_terms(text=description)
        df["corpus_id"] = uid

        df.to_parquet(output_directory / f"{uid}.parquet")


def count_terms(text: str) -> pd.DataFrame:
    """Tokenize, count, and return table with term counts.

    :param text:
        Words to tokenize and count
    :return:
        Table with columns for individual terms and counts
    """
    term_counts = Counter(text.split())
    df = pd.DataFrame(term_counts.items(), columns=["term", "frequency"])

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


def append_idf(df: SparkDataFrame) -> SparkDataFrame:
    """Append columns to `df` for document frequency and inverse document frequency.

    :param df:
        Table with terms and corpus unique ID to extract document frequency.
        Requires columns:
            * term
            * corpus_id
    :return:
        Original table `df` with extra columns:
            * document_frequency
            * inverse_document_frequency
    """
    window = Window().partitionBy("term")
    df = df.withColumn(
        "document_frequency", F.count("corpus_id").over(window)
    ).withColumn("inverse_document_frequency", 1 / F.col("document_frequency"))

    return df


# if __name__ == "__main__":
#     append_idf()
