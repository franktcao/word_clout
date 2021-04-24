import math
import pprint
import numpy as np


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
        return doc_count / n_posts
    elif method == "smooth":
        return (1 + doc_count) / n_posts
    elif method == "max":
        max_doc_count = kwargs.get("max_doc_count", None)
        if not max_doc_count:
            raise ValueError(
                "Since 'max' method chosen, 'max_doc_count' must be provided as keyword"
                " argument."
            )
        return (1 + doc_count) / max_doc_count
    elif method == "prob":
        return doc_count / (n_posts - doc_count + 1)
    else:
        raise ValueError("`method` must be one of 'standard', 'smooth', 'max', 'prob'.")


def get_inverse_doc_frequency(
    doc_freq: float, rescale_method: str = "standard"
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
    if rescale_method == "standard":
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
        return term_count / float(tot_term_count)
    elif method == "log_norm":
        return 1 + math.log(1 + term_count)
    elif method == "double_k_norm":
        k = kwargs.get("k", None)
        if not k:
            raise ValueError(
                "Since `method` is 'double_k_norm', 'k' must be provided as keyword "
                "argument."
            )
        return k + k * (term_count / max_term_count)
    else:
        raise ValueError(
            "`method` must be one of 'standard', 'log_norm', or 'double_k_norm"
        )


