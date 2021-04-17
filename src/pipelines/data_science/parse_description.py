from collections import Counter

import pandas as pd
import pyspark.sql.functions as F
from pyspark.shell import sqlContext
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.window import Window
from tqdm import tqdm

from src.definitions import TERM_FREQ_DIR


def convert_description_stats(data: pd.DataFrame) -> SparkDataFrame:
    """Convert job descriptions to term frequency counts.

    :param data:
        Table with job description columns
    :return:
        Table with term frequency counts
    """
    for row in tqdm(data.to_dict(orient="records"), desc="Row"):
        extract_description_stats(row)

    return sqlContext.read.load(str(TERM_FREQ_DIR))


def clean_description(text: str) -> str:
    """Clean description of characters to improve tokenization.

    :param text:
        Description text to clean
    :return:
        Cleaned description text
    """
    text = text.replace("\n", " ")
    text = text.replace("\t", " ")

    return text


def extract_description_stats(row: pd.Series) -> None:
    """Convert description in `row` to table with term frequency counts as parquet file.

    :param row:
        Record with job description and unique ID
    """
    uid = row["link"]
    description = row["description"]

    description = clean_description(description)
    description = description.lower()

    term_counts = Counter(description.split())

    df = pd.DataFrame(term_counts.items(), columns=["term", "frequency"])
    df["corpus_id"] = uid

    df.to_parquet(TERM_FREQ_DIR / f"{uid}.parquet")


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
