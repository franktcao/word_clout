import math
import pprint
import numpy as np


def get_document_frequency(
    doc_count: int, n_posts: int = 1, method: str = "standard", **kwargs,
) -> float:
    """Return document frequency of a term.
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


