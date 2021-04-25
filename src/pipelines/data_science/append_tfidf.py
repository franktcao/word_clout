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


def _doc_freq_standard(doc_count: int, n_posts: int) -> float:
    return doc_count / n_posts


def _doc_freq_smooth(doc_count: int, n_posts: int) -> float:
    return (1 + doc_count) / n_posts


def _doc_freq_max(doc_count: int, **kwargs) -> float:
    max_doc_count = kwargs.get("max_doc_count", None)
    if not max_doc_count:
        raise ValueError(
            "Since 'max' method chosen, 'max_doc_count' must be provided as keyword"
            " argument."
        )
    return (1 + doc_count) / max_doc_count


def _doc_freq_prob(doc_count: int, n_posts: int) -> float:
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


def _term_freq_standard(term_count: int, tot_term_count: int) -> float:
    return term_count / float(tot_term_count)


def _term_freq_log_norm(term_count: int) -> float:
    return 1 + math.log(1 + term_count)


def _term_freq_double_k_norm(term_count: int, **kwargs) -> float:
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
    return k + k * (term_count / max_term_count)
