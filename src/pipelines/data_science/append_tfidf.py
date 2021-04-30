import math
from typing import Optional


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


# Not covered: Simple calculation
def _doc_freq_standard(doc_count: int, n_posts: int) -> float:  # pragma: no cover
    return doc_count / n_posts


# Not covered: Simple calculation
def _doc_freq_smooth(doc_count: int, n_posts: int) -> float:  # pragma: no cover
    return (1 + doc_count) / n_posts


# Not covered: Simple calculation
def _doc_freq_max(doc_count: int, **kwargs) -> float:  # pragma: no cover
    max_doc_count = kwargs.get("max_doc_count", None)
    if not max_doc_count:
        raise ValueError(
            "Since 'max' method chosen, 'max_doc_count' must be provided as keyword"
            " argument."
        )
    return (1 + doc_count) / max_doc_count


# Not covered: Simple calculation
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
