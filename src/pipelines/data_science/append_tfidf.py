import math
from typing import Optional
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
import itertools
from .parse_description import _validate_has_columns
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window


def append_tf(df: SparkDataFrame) -> SparkDataFrame:
    _validate_has_columns(df, ["count_in_document", "corpus_id"])

    window = Window.partitionBy("corpus_id")
    result = df.withColumn(
        "total_terms_in_document", F.sum(F.col("count_in_document")).over(window)
    ).withColumn(
        "max_count_in_document", F.max(F.col("count_in_document")).over(window)
    )

    methods = ["standard", "smooth", "max", "prob"]
    method_to_kwargs = {"max": {"k": 0.5}}
    for method in methods:
        new_column = f"tf_{method}"
        kwargs = method_to_kwargs.get(method, dict())
        doc_freq_udf = F.udf(
            lambda term_count, tot_term_count, max_term_count: get_term_frequency(
                term_count,
                tot_term_count,
                max_term_count=max_term_count,
                method=method,
                **kwargs,
            ),
            T.FloatType(),
        )
        result = result.withColumn(
            new_column,
            doc_freq_udf(
                "count_in_document", "total_terms_in_document", "max_term_in_counts"
            ),
        )
    return result


def get_document_frequency(
    doc_count: int, n_posts: int = 1, method: str = "standard", **kwargs,
) -> float:
    """
    Return document frequency of a term.

    :param doc_count:
        Number of documents (from a corpus) where a given term appears in
    :param n_posts:
        Total number of documents in corpus
    :param method:
        Method or strategy to calculate the document frequency. Must be one of:
            * standard
            * smooth
            * max
            * prob
        See https://en.wikipedia.org/wiki/Tf%E2%80%93idf for more information.
    :param kwargs:
        Additional keyword arguments needed to calculate method of document frequency
    :return:
        Value of document frequency for the given method
    """
    if method == "standard":
        return _doc_freq_standard(doc_count, n_posts)
    elif method == "smooth":
        return _doc_freq_smooth(doc_count, n_posts)
    elif method == "max":
        return _doc_freq_max(doc_count, **kwargs)
    elif method == "prob":
        return _doc_freq_prob(doc_count, n_posts)
    else:
        raise ValueError("`method` must be one of 'standard', 'smooth', 'max', 'prob'.")


# Not covered: Simple helper calculation
def _doc_freq_standard(doc_count: int, n_posts: int) -> float:  # pragma: no cover
    return doc_count / n_posts


# Not covered: Simple helper calculation
def _doc_freq_smooth(doc_count: int, n_posts: int) -> float:  # pragma: no cover
    return (1 + doc_count) / n_posts


# Not covered: Simple helper calculation
def _doc_freq_max(doc_count: int, **kwargs) -> float:  # pragma: no cover
    max_doc_count = kwargs.get("max_doc_count", None)
    if not max_doc_count:
        raise ValueError(
            "Since 'max' method chosen, 'max_doc_count' must be provided as keyword"
            " argument."
        )
    return (1 + doc_count) / max_doc_count


# Not covered: Simple helper calculation
def _doc_freq_prob(doc_count: int, n_posts: int) -> float:  # pragma: no cover
    return doc_count / (n_posts - doc_count + 1)


def get_inverse_doc_frequency(
    doc_freq: float, rescale_method: Optional[str] = None
) -> float:
    """
    Return inverse document frequency (IDF).

    :param doc_freq:
        Value of document frequency
    :param rescale_method:
        Method used to rescale IDF. Must be one of:
            * standard
            * log
    :return:
        Value of IDF given the given method
    """
    inv_doc_freq = 1 / doc_freq
    if rescale_method is None:
        return inv_doc_freq
    elif rescale_method == "log":
        return math.log(inv_doc_freq)
    else:
        raise ValueError("`rescale_method` must be one of 'standard' or 'log'.")


def get_term_frequency(
    term_count: int, tot_term_count: int = 1, method: str = "standard", **kwargs,
) -> float:
    """
    Return the term frequency (TF) for a given method.

    :param term_count:
        Number of times the term appears in a given document
    :param tot_term_count:
        Total number of terms in given document
    :param method:
        Method or strategy to used to calculate term frequency. Must be one of:
            * standard
            * log_norm
            * double_k_norm
        See https://en.wikipedia.org/wiki/Tf%E2%80%93idf for more information.
    :param kwargs:
        Additional keyword arguments needed to calculate method of term frequency
    :return:
        Value of term frequency for a given method
    """
    if method == "standard":
        return _term_freq_standard(term_count, tot_term_count)
    elif method == "log_norm":
        return _term_freq_log_norm(term_count)
    elif method == "double_k_norm":
        return _term_freq_double_k_norm(term_count, **kwargs)
    else:
        raise ValueError(
            "`method` must be one of 'standard', 'log_norm', or 'double_k_norm'"
        )


# Not covered: Simple helper calculation
def _term_freq_standard(
    term_count: int, tot_term_count: int
) -> float:  # pragma: no cover
    return term_count / float(tot_term_count)


# Not covered: Simple helper calculation
def _term_freq_log_norm(term_count: int) -> float:  # pragma: no cover
    return 1 + math.log(1 + term_count)


# Not covered: Simple helper calculation
def _term_freq_double_k_norm(term_count: int, **kwargs) -> float:  # pragma: no cover
    k = kwargs.get("k", None)
    max_term_count = kwargs.get("max_term_count", None)
    if not k or not max_term_count:
        raise ValueError(
            "Since `method` is 'double_k_norm', both 'k' and 'max_term_count' must "
            "be provided as keyword arguments."
            "\nRequired keyword arguments:"
            f"\n\tk: {k}\n\tmax_term_count: {max_term_count}"
            f"\n\tPassed in keyword arguments: \n\t\t{kwargs}"
        )
    return k + (1 - k) * (term_count / max_term_count)
