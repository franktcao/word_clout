import math


class TermFrequencyCalculator:
    """
    Calculate different term frequency strategies.

    See https://en.wikipedia.org/wiki/Tf%E2%80%93idf for more information.
    """

    def __init__(
        self, term_count: int, terms_in_document: int, max_term_count: int
    ) -> None:
        """
        :param term_count:
            Number of times a given term appears in a given document
        :param terms_in_document:
            Total number of terms in a given document
        :param max_term_count:
            Greatest term count in a given document
        """
        if term_count <= 0 or not isinstance(term_count, int):
            raise ValueError(
                f"`terms_in_document` must be a positive integer."
                f"\nPassed in: {term_count}"
            )
        if terms_in_document <= 0 or not isinstance(terms_in_document, int):
            raise ValueError(
                f"`terms_in_document` must be a positive integer."
                f"\nPassed in: {terms_in_document}"
            )
        if max_term_count <= 0 or not isinstance(max_term_count, int):
            raise ValueError(
                f"`max_term_count` must be a positive integer."
                f"\nPassed in: {max_term_count}"
            )

        self.term_count = term_count
        self.tot_term_count = terms_in_document
        self.max_term_count = max_term_count

    # Not covered: Simple calculation
    @property
    def binary(self) -> float:  # pragma: no cover
        return 1

    # Not covered: Simple calculation
    @property
    def raw_count(self) -> float:  # pragma: no cover
        return self.term_count

    # Not covered: Simple calculation
    @property
    def term_frequency(self) -> float:  # pragma: no cover
        return self.term_count / float(self.tot_term_count)

    # Not covered: Simple calculation
    @property
    def log_normalization(self) -> float:  # pragma: no cover
        return 1 + math.log(1 + self.term_count)

    # Not covered: Simple calculation
    @property
    def double_normalization_half(self) -> float:  # pragma: no cover
        return self.get_double_k_normalization(k=0.5)

    # Not covered: Simple calculation
    def get_double_k_normalization(self, k: float) -> float:  # pragma: no cover
        if not 0 < k < 1:
            raise ValueError(f"`k` must be positive but less than 1. \nPassed in: {k}")
        return (1 - k) * (self.term_count / self.max_term_count) + k
