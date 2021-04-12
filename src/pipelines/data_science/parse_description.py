from collections import Counter

import pandas as pd
import pyspark.sql.functions as F
from pyspark.shell import sqlContext
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.window import Window
from tqdm import tqdm

from src.definitions import INV_DOC_FREQ_DIR, TERM_FREQ_DIR


# from kedro.extras.datasets.spark import SparkDataSet
import pyspark.sql.types as T


def convert_description_stats(data: pd.DataFrame) -> SparkDataFrame:
    schema = T.StructType(
        [
            T.StructField("term", T.StringType()),
            T.StructField("frequency", T.IntegerType()),
            T.StructField("corpus_id", T.StringType()),
        ]
    )
    df: SparkDataFrame = sqlContext.createDataFrame([], schema=schema)
    for row in tqdm(data.to_dict(orient="records"), desc="Row"):
        df = df.unionByName(extract_description_stats(row))

    # return sqlContext.read.load(str(TERM_FREQ_DIR))
    return df


def clean_description(text: str) -> str:
    text = text.replace("\n", " ")
    text = text.replace("\t", " ")

    return text


def extract_description_stats(row: pd.Series) -> SparkDataFrame:
    uid = row["link"]
    description = row["description"]

    description = clean_description(description)
    description = description.lower()

    term_counts = Counter(description.split())

    df = sqlContext.createDataFrame(
        term_counts.items(),
        schema=T.StructType(
            [
                T.StructField("term", T.StringType()),
                T.StructField("frequency", T.IntegerType()),
            ]
        ),
    )
    # df["corpus_id"] = uid
    df = df.withColumn("corpus_id", F.lit(uid))

    # df.to_parquet(TERM_FREQ_DIR / f"{uid}.parquet")
    # return sqlContext.createDataFrame(df)
    return df


def append_idf() -> None:
    df: SparkDataFrame = sqlContext.read.load(str(TERM_FREQ_DIR))
    # df: SparkDataFrame = SparkDataSet(str(TERM_FREQ_DIR)).load()

    window = Window().partitionBy("term")
    df = df.withColumn(
        "document_frequency", F.count("corpus_id").over(window)
    ).withColumn("inverse_document_frequency", 1 / F.col("document_frequency"))

    df.write.partitionBy("corpus_id").mode("overwrite").parquet(str(INV_DOC_FREQ_DIR))
    # SparkDataSet(str(INV_DOC_FREQ_DIR)).save(df)


# if __name__ == "__main__":
#     append_idf()
