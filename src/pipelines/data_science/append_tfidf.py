from typing import List

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

from ...calculators import (
    IdfScale,
    InverseDocumentFrequencyCalculator,
    TermFrequencyCalculator,
)


def append_tfidf_scores(data: SparkDataFrame) -> SparkDataFrame:
    ks = [0.0, 0.2, 0.8, 1.0]

    @F.pandas_udf(f=data.schema, functionType=F.PandasUDFType.GROUPED_MAP)
    def append_tf_scores_udf(pdf: pd.DataFrame) -> pd.DataFrame:
        return append_tf_scores(pdf, ks=ks)

    result = data.groupby("corpus_id").apply(append_tf_scores_udf)

    @F.pandas_udf(f=result.schema, functionType=F.PandasUDFType.GROUPED_MAP)
    def append_idf_scores_log_udf(pdf: pd.DataFrame) -> pd.DataFrame:
        return append_idf_scores(pdf)

    result = result.groupby("corpus_id").apply(append_idf_scores_log_udf)

    @F.pandas_udf(f=result.schema, functionType=F.PandasUDFType.GROUPED_MAP)
    def append_idf_scores_udf(pdf: pd.DataFrame) -> pd.DataFrame:
        return append_idf_scores(pdf, scale=IdfScale.STANDARD)

    result = result.groupby("corpus_id").apply(append_idf_scores_udf)

    return result


def append_tf_scores(data: pd.DataFrame, ks: List[float]) -> pd.DataFrame:
    result = data.apply(lambda row: append_tf_scores_by_row(row=row, ks=ks), axis=0)
    return result


def append_tf_scores_by_row(row: pd.Series, ks: List[float]) -> pd.Series:
    raw_term_counts = row["count_in_document"]
    terms_in_document = row["terms_in_document"]
    max_term_count = row["max_term_count"]

    tf_scores = TermFrequencyCalculator(
        term_count=raw_term_counts,
        terms_in_document=terms_in_document,
        max_term_count=max_term_count,
    )
    name_to_score = {
        "tf_binary": tf_scores.binary,
        "tf_raw_count": tf_scores.raw_count,
        "tf_term_frequency": tf_scores.term_frequency,
        "tf_log_normalization": tf_scores.log_normalization,
        "tf_double_normalization_half": tf_scores.double_normalization_half,
    }

    k_scores = {
        f"tf_double_normalization__k={k:.2f}": tf_scores.get_double_k_normalization(k)
        for k in ks
    }

    return pd.Series({**name_to_score, **k_scores})


def append_idf_scores(
    data: pd.DataFrame, scale: IdfScale = IdfScale.LOG
) -> pd.DataFrame:
    result = data.apply(
        lambda row: append_idf_scores_by_row(row=row, scale=scale), axis=0
    )
    return result


def append_idf_scores_by_row(
    row: pd.Series, scale: IdfScale = IdfScale.LOG
) -> pd.Series:
    documents_with_term = row["documents_with_term"]
    documents_in_corpus = row["documents_in_corpus"]
    max_doc_count = row["max_doc_count"]

    idf_scores = InverseDocumentFrequencyCalculator(
        documents_with_term=documents_with_term,
        documents_in_corpus=documents_in_corpus,
        max_doc_count=max_doc_count,
        scale=scale,
    )
    suffix = "_log" if scale == IdfScale.LOG else ""
    name_to_score = {
        f"idf_unary{suffix}": idf_scores.unary,
        f"idf_document_frequency{suffix}": idf_scores.document_frequency,
        f"idf_smooth{suffix}": idf_scores.smooth,
        f"idf_max{suffix}": idf_scores.max,
        f"idf_probabilistic{suffix}": idf_scores.probabilistic,
    }

    return pd.Series({**name_to_score,})
